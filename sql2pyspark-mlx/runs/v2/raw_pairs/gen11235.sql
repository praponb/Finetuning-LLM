WITH product_totals AS (
    SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM order_items
    WHERE order_id IN (15, 16, 39)
    GROUP BY product_id
)
SELECT p.supplier_id, pt.quantity_range
FROM products p
JOIN product_totals pt ON p.id = pt.product_id;
