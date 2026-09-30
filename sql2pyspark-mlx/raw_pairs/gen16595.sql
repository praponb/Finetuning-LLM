WITH flattened AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS attr_key, attr_value
),
filtered AS (
    SELECT
        *,
        SORT_ARRAY(coupon_codes) AS sorted_coupon_codes,
        COALESCE(attr_key, 'UNKNOWN') AS attr_key_filled
    FROM flattened
    WHERE attr_key IS NOT NULL
      AND attr_key IS DISTINCT FROM 'gift'
),
valid AS (
    SELECT
        id,
        customer_id,
        attr_key,
        attr_value,
        sorted_coupon_codes,
        SIZE(coupon_codes) AS coupon_codes_count
    FROM filtered
    WHERE customer_id IS NOT NULL
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY coupon_codes_count DESC NULLS FIRST) AS coupon_codes_count_dense_rank,
        CUME_DIST() OVER (PARTITION BY attr_key, customer_id ORDER BY coupon_codes_count DESC NULLS LAST) AS coupon_codes_count_cume_dist,
        PERCENT_RANK() OVER w_grp AS coupon_codes_count_percent_rank
    FROM valid
    WHERE coupon_codes_count <= 57
    WINDOW w_grp AS (PARTITION BY attr_key ORDER BY coupon_codes_count DESC NULLS FIRST)
)
SELECT
    CASE WHEN coupon_codes_count_dense_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END AS coupon_codes_count_dense_rank_tier,
    customer_id,
    COUNT(CASE WHEN attr_key = 'utm_source' THEN 1 END) AS utm_source_count,
    COUNT(*) AS cnt,
    MAX(coupon_codes_count) AS max_coupon_codes_count
FROM scored
GROUP BY coupon_codes_count_dense_rank_tier, customer_id
HAVING cnt > 1
ORDER BY cnt DESC;
