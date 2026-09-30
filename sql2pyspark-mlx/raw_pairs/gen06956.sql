WITH latest_match AS (
    SELECT
        s.id,
        s.rating,
        latest.product_price
    FROM suppliers s
    JOIN LATERAL (
            SELECT c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT id
FROM latest_match
