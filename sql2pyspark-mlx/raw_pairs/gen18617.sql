WITH lines AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS k, v
)
SELECT
    customer_id,
    k,
    COUNT(DISTINCT id) AS distinct_ids,
    SIZE(COLLECT_SET(k)) AS num_ks,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list,
    COUNT(DISTINCT id) AS distinct_ids_2,
    COUNT(DISTINCT k) AS distinct_ks
FROM lines
GROUP BY CUBE (customer_id, k)
ORDER BY num_ks NULLS FIRST
