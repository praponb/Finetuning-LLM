SELECT *
FROM (
    SELECT order_id, MAX(quantity) AS max_quantity, AVG(quantity) AS avg_quantity
    FROM order_items
    GROUP BY order_id
) AS sub
WHERE avg_quantity < 5.7
ORDER BY order_id DESC, avg_quantity DESC
