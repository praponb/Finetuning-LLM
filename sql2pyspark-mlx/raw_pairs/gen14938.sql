WITH flattened AS (
    SELECT
        id,
        created_at,
        code
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) e AS code
),
cleaned AS (
    SELECT
        *,
        COALESCE(code, 'unassigned') AS code_filled,
        COALESCE(code, 'UNKNOWN') AS code_clean
    FROM flattened
    WHERE created_at >= DATE '2025-03-01'
      AND code IS DISTINCT FROM 'VIP'
),
base AS (
    SELECT
        id,
        created_at,
        code_filled,
        code_clean,
        LOWER(code) AS code_lower,
        CASE WHEN code IN ('WELCOME5', 'BUNDLE') THEN 'Tier 1' ELSE 'Other' END AS code_group
    FROM cleaned
    WHERE code IS NOT NULL
),
cleaned_carts AS (
    SELECT *, CAST(created_at AS DATE) AS created_at_day
    FROM base
    WHERE created_at > TO_TIMESTAMP('2025-03-02 01:00:00')
),
filtered AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at
    FROM cleaned_carts
    WHERE created_at > DATE '2025-03-01'
),
summary AS (
    SELECT
        DATE_TRUNC('HOUR', created_at) AS hour_window,
        CASE WHEN days_since_created_at >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END AS days_since_created_at_tier,
        MIN(days_since_created_at) AS min_days_since_created_at,
        ROUND(AVG(days_since_created_at), 2) AS avg_days_since_created_at,
        (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
        MAX(created_at) AS last_seen,
        APPROX_PERCENTILE(days_since_created_at, ARRAY(0.25, 0.5, 0.75)) AS days_since_created_at_quartiles
    FROM filtered
    GROUP BY 1, 2
)
SELECT
    hour_window,
    days_since_created_at_tier,
    min_days_since_created_at,
    avg_days_since_created_at,
    last_seen
FROM summary
ORDER BY last_seen DESC, hour_window DESC;
