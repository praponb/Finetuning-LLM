WITH scoped AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        TRUNC(claim_date, 'MM') AS claim_date_month_start,
        ABS(claimed_amount - 20461.6) AS claimed_amount_distance
    FROM claims
    WHERE claim_type IN ('flood', 'theft')
      AND status = 'REJECTED'
),
deduped AS (
    -- Keep the latest row per claim_type, status
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        claim_date_month_start,
        claimed_amount_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type, status ORDER BY claimed_amount_distance DESC, id DESC) AS rn
        FROM scoped
    ) ranked
    WHERE rn = 1
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_date,
        (SELECT MIN(c.approved_amount) FROM claims c WHERE c.policy_id = s.policy_id) AS min_claim_approved_amount
    FROM deduped s
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM with_counts
    WHERE min_claim_approved_amount > (SELECT MIN(min_claim_approved_amount) FROM with_counts)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(claim_type, '#', CAST(policy_id AS STRING)) AS claim_type_ref,
        DATEDIFF(last_claim_date, claim_date_month_start) AS days_claim_date_month_start_to_last_claim_date,
        ABS(claim_count - 3) AS claim_count_distance
    FROM without_matches
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_date_2,
        (SELECT ROUND(AVG(c.claimed_amount), 2) FROM claims c WHERE c.policy_id = s.policy_id) AS avg_claim_claimed_amount,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count_2
    FROM labeled s
)
SELECT DISTINCT adjuster_id
FROM summarized_rows
ORDER BY adjuster_id DESC
