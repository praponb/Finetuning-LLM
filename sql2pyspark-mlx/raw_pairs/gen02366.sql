WITH lines AS (
    SELECT
        id,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS k, attr_value
),
remaining_keys AS (
    SELECT id
    FROM lines
    INTERSECT
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
),
kept_rows AS (
    SELECT s.*
    FROM lines s
    JOIN remaining_keys k ON s.id = k.id
),
cleaned AS (
    SELECT *, SIZE(coupon_codes) coupon_codes_count
    FROM kept_rows
    WHERE SIZE(coupon_codes) > 1
),
with_windows AS (
    SELECT *, AVG(coupon_codes_count) OVER w_grp k_avg_coupon_codes_count
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY k)
),
cleaned_carts AS (
    SELECT
        id,
        coupon_codes,
        k,
        attr_value,
        k_avg_coupon_codes_count,
        ROUND(k_avg_coupon_codes_count, 0) k_avg_coupon_codes_count_rounded
    FROM with_windows
    WHERE k IS NOT NULL
),
base AS (
    SELECT
        *,
        SIZE(coupon_codes) coupon_codes_count,
        COALESCE(k, 'NONE') resolved_k
    FROM cleaned_carts
    WHERE k_avg_coupon_codes_count_rounded IS NOT NULL
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY k_avg_coupon_codes_count) k_avg_coupon_codes_count_dense_rank,
        MIN(k_avg_coupon_codes_count) OVER (PARTITION BY k) k_min_k_avg_coupon_codes_count
    FROM base
)
SELECT
    k,
    COUNT(CASE WHEN k = 'weight_kg' THEN 1 END) AS weight_kg_count,
    COUNT(DISTINCT k) AS distinct_ks,
    PERCENTILE_APPROX(k_avg_coupon_codes_count_rounded, 0.5) AS k_avg_coupon_codes_count_rounded_p50,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(coupon_codes_count), 2) AS avg_coupon_codes_count,
    COUNT(CASE WHEN k = 'utm_source' THEN 1 END) AS utm_source_count
FROM scored
GROUP BY k
HAVING COUNT(CASE WHEN k = 'utm_source' THEN 1 END) >= 1
ORDER BY distinct_ks NULLS LAST;
