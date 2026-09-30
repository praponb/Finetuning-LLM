WITH joined AS (
    SELECT
        a.*,
        b.supplier_name,
        b.country,
        b.city
    FROM products a
    LEFT JOIN suppliers b
        ON a.supplier_id = b.id
),
kept AS (
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.product_id IN (8, 39)
        )
),
with_latest AS (
    SELECT s.*, top1.last_restock
    FROM kept s
    JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    s.id,
    s.product_name,
    s.brand,
    s.price,
    s.cost,
    s.launch_date,
    s.tags,
    s.supplier_id,
    s.supplier_name,
    (SELECT AVG(x.stock_qty) FROM products x WHERE x.supplier_id = s.supplier_id) AS avg_product_stock_qty
FROM with_latest s
ORDER BY launch_date DESC NULLS FIRST, cost DESC
