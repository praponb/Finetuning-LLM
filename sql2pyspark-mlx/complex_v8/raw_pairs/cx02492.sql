WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        DAYOFWEEK(reported_at) AS reported_at_dayofweek,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount
    FROM claims
    WHERE claimed_amount <= 31752.9
      AND HOUR(reported_at) BETWEEN 6 AND 9
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MIN(claimed_amount) AS min_claim_claimed_amount,
        MAX(reported_at) AS last_claim_reported_at
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.min_claim_claimed_amount,
        c.last_claim_reported_at
    FROM filtered_claims s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_counts s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.approved_amount < 20880.4
        )
),
enriched_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.reported_at_dayofweek,
        s.least_approved_amount_claimed_amount,
        s.min_claim_claimed_amount,
        s.last_claim_reported_at,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM qualified s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
claim_stats_2 AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(CASE WHEN status IS NOT NULL THEN 1 END) AS claim_status_count
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_status_count, 0) AS claim_status_count
    FROM enriched_adjuster s
    LEFT JOIN claim_stats_2 c ON c.policy_id = s.policy_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        claim_date,
        approved_amount,
        reported_at,
        reported_at_dayofweek,
        least_approved_amount_claimed_amount,
        min_claim_claimed_amount,
        last_claim_reported_at,
        adjuster_name,
        adjuster_level,
        claim_status_count,
        approved_amount + adjuster_level AS approved_amount_plus_adjuster_level,
        INITCAP(TRIM(status)) AS status_clean
    FROM summarized_rows
    WHERE approved_amount >= 0.0
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(min_claim_claimed_amount * 1.5, 2) AS min_claim_claimed_amount_adj
    FROM labeled
    WHERE approved_amount_plus_adjuster_level >= 17501.8
)
SELECT
    id,
    claim_date,
    approved_amount,
    reported_at,
    reported_at_dayofweek,
    least_approved_amount_claimed_amount,
    min_claim_claimed_amount,
    last_claim_reported_at,
    adjuster_name,
    adjuster_level,
    claim_status_count,
    status_clean,
    min_claim_claimed_amount_adj,
    ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(last_claim_reported_at)) / 60, 1) AS minutes_last_claim_reported_at_to_reported_at
FROM derived
WHERE claim_status_count < 6
ORDER BY claim_status_count, min_claim_claimed_amount_adj DESC
