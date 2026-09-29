SELECT
    order_id,
    product_id,
    SUM(quantity) AS total_quantity,
    COUNT(quantity) AS quantity_count
FROM order_items
WHERE unit_price > 300 AND quantity > 4 AND product_id = 32
GROUP BY order_id, product_id
HAVING COUNT(quantity) < 2
ORDER BY 2 ASC, order_id ASC
LIMIT 10
