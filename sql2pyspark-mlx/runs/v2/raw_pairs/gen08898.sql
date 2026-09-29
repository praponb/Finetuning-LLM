WITH ProductSummary AS (
    SELECT product_id, AVG(quantity) AS avg_quantity, COUNT(quantity) AS quantity_count
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.stock_qty,
    p.launch_date,
    P.avg_quantity,
    P.quantity_count
FROM products p
LEFT JOIN ProductSummary P ON p.id = P.product_id
