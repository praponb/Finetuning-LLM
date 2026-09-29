WITH scoped_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM claims
    WHERE approved_amount < 14668.0
      AND policy_id >= 5
),
with_activity AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM scoped_claims s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.start_date >= DATE '2025-01-24'
        )
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        FIRST_VALUE(reported_at) OVER (PARTITION BY policy_id ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at,
        MAX(claimed_amount) OVER (PARTITION BY policy_id) AS policy_id_max_claimed_amount
    FROM with_activity
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.approved_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS min_claim_approved_amount,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count
    FROM sequenced s
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY status ORDER BY claim_count ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MIN(claim_count) OVER (ORDER BY first_reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claim_count_so_far,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY status), 2) AS claimed_amount_vs_status_avg,
        LAST_VALUE(claim_count) OVER (ORDER BY first_reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_count
    FROM profiled
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        claim_type,
        status,
        reported_at,
        first_reported_at,
        policy_id_max_claimed_amount,
        claim_count,
        min_claim_count_so_far,
        MAX(policy_id_max_claimed_amount) OVER (PARTITION BY status, claim_count) AS status_claim_count_max_policy_id_max_claimed_amo,
        LEAD(claim_count, 3) OVER (PARTITION BY TO_DATE(first_reported_at), adjuster_id ORDER BY policy_id_max_claimed_amount ASC, id) AS next3_claim_count,
        COUNT(*) OVER (PARTITION BY status, claim_count) AS rows_in_status_claim_count,
        LEAD(claimed_amount, 3, 0) OVER (PARTITION BY DATE_TRUNC('MONTH', first_reported_at), last_claim_count ORDER BY last_claim_count DESC, id) AS next3_claimed_amount
    FROM with_trends
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_type,
    status,
    first_reported_at,
    policy_id_max_claimed_amount,
    min_claim_count_so_far,
    status_claim_count_max_policy_id_max_claimed_amo,
    next3_claim_count,
    rows_in_status_claim_count,
    next3_claimed_amount,
    CONCAT(claim_type, '#', CAST(min_claim_count_so_far AS STRING)) AS claim_type_ref,
    TO_DATE(first_reported_at) AS first_reported_at_date
FROM with_windows
ORDER BY status_claim_count_max_policy_id_max_claimed_amo DESC, next3_claim_count DESC
