WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS k, value
)
-- Aggregate with conditional metrics
SELECT
    k,
    SIZE(COLLECT_SET(k)) AS num_ks,
    SIZE(COLLECT_SET(k)) AS num_ks_2,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list_2,
    SIZE(COLLECT_SET(k)) AS num_ks_3
FROM exploded
GROUP BY k
