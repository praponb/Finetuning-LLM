WITH filtered AS (
    SELECT *, CAST(is_active AS INT) AS is_active_flag
    FROM customers
    WHERE signup_date >= TO_DATE('2023-04-17')
      AND segment IS NOT NULL
),
with_last AS (
    SELECT
        s.*,
        x.device,
        x.browser
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.device, c.browser
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept_keys AS (
    SELECT id
    FROM with_last
    INTERSECT
    SELECT id
    FROM customers
    WHERE is_active
),
verified AS (
    SELECT s.*
    FROM with_last s
    JOIN kept_keys k ON s.id = k.id
),
matched_keys AS (
    SELECT country, id
    FROM verified
    EXCEPT ALL
    SELECT country, id
    FROM customers
    WHERE city IN ('Tokyo', 'Bangkok')
),
verified_customers AS (
    SELECT s.*
    FROM verified s
    JOIN matched_keys k ON s.country = k.country AND s.id = k.id
),
kept AS (
    SELECT s.*
    FROM verified_customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date >= s.signup_date
        )
)
SELECT
    CASE WHEN credit_limit >= 17200 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    DATE_TRUNC('WEEK', signup_date) AS week_start,
    APPROX_PERCENTILE(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
    SUM(credit_limit) FILTER (WHERE browser = 'Edge') AS edge_total_credit_limit,
    COUNT(DISTINCT segment) AS distinct_segments,
    APPROX_PERCENTILE(is_active_flag, ARRAY(0.25, 0.5, 0.75)) AS is_active_flag_quartiles,
    SUM(credit_limit) AS total_credit_limit,
    COUNT_IF(device = 'mobile') AS mobile_events
FROM kept
GROUP BY CASE WHEN credit_limit >= 17200 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', signup_date)
ORDER BY week_start;
