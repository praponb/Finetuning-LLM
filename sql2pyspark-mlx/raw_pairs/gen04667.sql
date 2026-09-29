WITH not_flagged AS (
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.order_id IN (3, 16, 38, 40)
        )
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.brand,
    s.price,
    s.cost,
    s.launch_date,
    s.supplier_id,
    (SELECT MIN(x.price) FROM products x WHERE x.supplier_id = s.supplier_id) AS min_product_price
FROM not_flagged s
ORDER BY min_product_price, price DESC;
