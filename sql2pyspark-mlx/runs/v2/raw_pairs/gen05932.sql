WITH ProductTotals AS (
    SELECT product_id, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM order_items
    GROUP BY product_id
)
SELECT p.category, p.supplier_id, P.avg_quantity
FROM products p
INNER JOIN ProductTotals P ON p.id = P.product_id
