WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd HH:00') AS reported_at_label,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM claims
    WHERE claim_date < DATE '2025-04-14'
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MIN(claim_date) AS first_claim_date,
        COUNT(*) AS claim_count,
        COUNT(*) FILTER (WHERE status IS NOT NULL) AS claim_status_count
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_date,
        COALESCE(c.claim_count, 0) AS claim_count,
        COALESCE(c.claim_status_count, 0) AS claim_status_count
    FROM scoped s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
SELECT
    COALESCE(status, 'ALL') AS status,
    claim_status_count,
    policy_id,
    ROUND(STDDEV(claimed_amount), 2) AS stddev_claimed_amount,
    MAX(claim_count) AS max_claim_count
FROM summarized_rows
GROUP BY GROUPING SETS ((status, claim_status_count, policy_id), (status))
ORDER BY status NULLS LAST, claim_status_count NULLS LAST, policy_id NULLS LAST
