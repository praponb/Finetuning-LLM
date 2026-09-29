WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, value
)
SELECT
    id,
    created_at,
    k,
    ROW_NUMBER() OVER (PARTITION BY TO_DATE(created_at), k ORDER BY created_at DESC, id ASC, k ASC) row_seq,
    LAST_VALUE(k, TRUE) OVER (PARTITION BY customer_id, k ORDER BY created_at ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_k,
    FIRST_VALUE(k) IGNORE NULLS OVER (PARTITION BY k ORDER BY created_at ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_k
FROM exploded
