WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.status,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) FILTER (WHERE status NOT IN ('APPROVED', 'IN_REVIEW')) AS claim_status_count,
        MIN(reported_at) AS first_claim_reported_at,
        COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_status_count, 0) AS claim_status_count,
        c.first_claim_reported_at,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM matched s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('WEEK', claim_date) AS claim_date_week,
        claim_count + premium AS claim_count_plus_premium
    FROM profiled
    WHERE claim_date BETWEEN DATE '2024-06-26' AND DATE '2025-09-19'
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(claim_date_week) AS claim_date_week_day,
        status,
        ROUND(100 * COUNT(CASE WHEN status IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_status_is_not_null,
        COUNT(*) AS row_count
    FROM tagged
    WHERE premium > 1132.1
    GROUP BY TO_DATE(claim_date_week), status
)
-- Final projection
SELECT
    claim_date_week_day,
    status,
    pct_status_is_not_null,
    row_count,
    COALESCE(pct_status_is_not_null, 0.0) AS pct_status_is_not_null_filled,
    row_count + pct_status_is_not_null AS row_count_plus_pct_status_is_not_null
FROM per_group
ORDER BY claim_date_week_day, status
