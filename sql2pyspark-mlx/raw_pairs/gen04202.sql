WITH lines AS (
    SELECT
        id,
        customer_id,
        key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS key, attr_value
),
filtered AS (
    SELECT
        *,
        UPPER(key) AS key_upper,
        CASE WHEN key = 'weight_kg' THEN 1 ELSE 0 END AS is_weight_kg
    FROM lines
    WHERE key IS NOT NULL
),
kept_keys AS (
    SELECT id, customer_id
    FROM filtered
    EXCEPT
    SELECT id, customer_id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
),
verified AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
enriched AS (
    SELECT
        id,
        customer_id,
        key,
        attr_value,
        key_upper,
        is_weight_kg,
        AVG(is_weight_kg) OVER (PARTITION BY customer_id) AS customer_id_avg_is_weight_kg,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY is_weight_kg DESC NULLS LAST) AS is_weight_kg_cume_dist,
        MIN(is_weight_kg) OVER (PARTITION BY customer_id) AS customer_id_min_is_weight_kg,
        COUNT(is_weight_kg) OVER (PARTITION BY key, customer_id) AS key_count_is_weight_kg,
        PERCENT_RANK() OVER (PARTITION BY key ORDER BY is_weight_kg DESC NULLS LAST) AS is_weight_kg_percent_rank
    FROM verified
),
summary AS (
    SELECT
        CASE WHEN customer_id_min_is_weight_kg >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END AS customer_id_min_is_weight_kg_tier,
        SUM(CASE WHEN key = 'device' THEN customer_id_avg_is_weight_kg ELSE 0 END) AS device_customer_id_avg_is_weight_kg,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids
    FROM enriched
    GROUP BY CASE WHEN customer_id_min_is_weight_kg >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    customer_id_min_is_weight_kg_tier,
    device_customer_id_avg_is_weight_kg,
    distinct_customer_ids
FROM summary
