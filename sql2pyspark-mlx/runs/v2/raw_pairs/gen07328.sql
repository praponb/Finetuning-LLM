WITH ProductStats AS (
    SELECT product_id, AVG(unit_price) AS avg_unit_price
    FROM order_items
    WHERE quantity IS NULL
    GROUP BY product_id
)
SELECT p.id, p.supplier_id, P.avg_unit_price
FROM products p
INNER JOIN ProductStats P ON p.id = P.product_id
ORDER BY supplier_id;
