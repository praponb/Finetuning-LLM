WITH lines AS (
    SELECT
        id,
        customer_id,
        code
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) e AS code
),
base AS (
    SELECT
        *,
        CASE WHEN code = 'VIP' THEN 1 ELSE 0 END AS is_vip,
        LOWER(code) AS code_lower,
        COALESCE(code, 'DIRECT') AS resolved_code
    FROM lines
    WHERE customer_id = 20
),
cleaned AS (
    SELECT *, COALESCE(code, 'unassigned') AS code_filled
    FROM base
    WHERE code IS DISTINCT FROM 'WELCOME5'
      AND code IS NOT NULL
),
cleaned_carts AS (
    SELECT
        *,
        FLOOR(is_vip) AS is_vip_whole,
        COALESCE(code, 'UNKNOWN') AS code_clean
    FROM cleaned
    WHERE code IS NOT NULL
      AND code IN ('SPRING10', 'VIP')
),
cleaned_carts_2 AS (
    SELECT
        id,
        customer_id,
        code,
        is_vip,
        code_lower,
        code_filled,
        is_vip_whole,
        code_clean,
        is_vip * is_vip_whole AS is_vip_times_is_vip_whole,
        CASE WHEN is_vip_whole >= 76 THEN 'High' WHEN is_vip_whole >= 35 THEN 'Medium' ELSE 'Low' END AS is_vip_whole_tier
    FROM cleaned_carts
    WHERE is_vip_whole BETWEEN 38 AND 53
),
with_windows AS (
    SELECT *, DENSE_RANK() OVER (ORDER BY is_vip_times_is_vip_whole ASC NULLS LAST) AS is_vip_times_is_vip_whole_dense_rank
    FROM cleaned_carts_2
    WHERE code IN ('VIP', 'SPRING10', 'BUNDLE')
),
filtered AS (
    SELECT *, ROUND(is_vip * 1.1, 2) AS is_vip_adjusted
    FROM with_windows
    WHERE customer_id IS NOT NULL
      AND code IN ('WELCOME5', 'SPRING10')
)
SELECT
    CASE WHEN is_vip_whole >= 69 THEN 'LARGE' ELSE 'SMALL' END AS is_vip_whole_tier,
    APPROX_PERCENTILE(is_vip_times_is_vip_whole, ARRAY(0.25, 0.5, 0.75)) AS is_vip_times_is_vip_whole_quartiles,
    COUNT(*) AS event_count,
    MIN(is_vip_times_is_vip_whole) AS min_is_vip_times_is_vip_whole,
    COUNT_IF(code = 'WELCOME5') AS welcome5_events
FROM filtered
GROUP BY CASE WHEN is_vip_whole >= 69 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT_IF(code = 'WELCOME5') > 1
ORDER BY is_vip_whole_tier DESC
