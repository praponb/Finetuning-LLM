WITH exploded AS (
    SELECT
        id,
        created_at,
        line.product_id,
        line.qty,
        line.price
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) li AS line
)
SELECT
    id,
    created_at,
    product_id,
    RANK() OVER (ORDER BY qty DESC NULLS FIRST) AS qty_rank,
    PERCENT_RANK() OVER (ORDER BY product_id DESC NULLS FIRST) AS product_id_percent_rank
FROM exploded
WHERE price <= 130
