WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS k, attr_value
)
-- Ranking and gap features
SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    k,
    attr_value,
    LAST_VALUE(k) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('month', created_at), customer_id ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_k,
    FIRST_VALUE(k) IGNORE NULLS OVER (ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k,
    FIRST_VALUE(k) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k_2,
    LAST_VALUE(k) IGNORE NULLS OVER (ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_k_2
FROM lines
