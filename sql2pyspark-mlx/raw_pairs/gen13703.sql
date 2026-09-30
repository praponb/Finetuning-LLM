WITH exploded AS (
    SELECT
        id,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS attr_key, value
),
exploded_carts AS (
    SELECT value, code
    FROM exploded
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS code
),
filtered AS (
    SELECT *, LOWER(code) code_lower
    FROM exploded_carts
    WHERE code IS DISTINCT FROM 'BUNDLE'
),
filtered_carts AS (
    SELECT
        *,
        CASE WHEN code = 'BUNDLE' THEN 1 ELSE 0 END is_bundle,
        COALESCE(code, 'DIRECT') code_filled
    FROM filtered
    WHERE code IS NOT NULL
      AND code IN ('VIP', 'WELCOME5', 'SPRING10')
),
enriched AS (
    SELECT
        *,
        STDDEV_SAMP(is_bundle) OVER (PARTITION BY code) code_stddev_samp_is_bundle,
        CUME_DIST() OVER (ORDER BY is_bundle) is_bundle_cume_dist,
        COUNT(is_bundle) OVER (PARTITION BY code) code_count_is_bundle,
        RANK() OVER (ORDER BY is_bundle DESC) is_bundle_rank
    FROM filtered_carts
)
SELECT
    CASE WHEN is_bundle_cume_dist >= 0.75 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_bundle_cume_dist_tier,
    CASE WHEN code_count_is_bundle >= 14 THEN 'PREMIUM' ELSE 'STANDARD' END AS code_count_is_bundle_tier,
    SORT_ARRAY(COLLECT_SET(code)) AS code_list,
    ROUND(SUM(is_bundle) / NULLIF(COUNT(CASE WHEN code = 'WELCOME5' THEN 1 END), 0), 2) AS is_bundle_per_welcome5,
    COUNT(CASE WHEN code = 'WELCOME5' THEN 1 END) AS welcome5_count,
    COUNT(DISTINCT code) AS distinct_codes,
    COUNT(DISTINCT code) AS distinct_codes_2,
    GROUPING(CASE WHEN is_bundle_cume_dist >= 0.75 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
FROM enriched
GROUP BY GROUPING SETS ((CASE WHEN is_bundle_cume_dist >= 0.75 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN code_count_is_bundle >= 14 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN is_bundle_cume_dist >= 0.75 THEN 'PREMIUM' ELSE 'STANDARD' END));
