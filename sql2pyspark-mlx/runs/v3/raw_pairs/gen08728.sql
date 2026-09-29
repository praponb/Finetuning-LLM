SELECT *
FROM (
    SELECT order_id, AVG(quantity) AS avg_quantity
    FROM order_items
    GROUP BY order_id
) AS s
WHERE avg_quantity < 7.7
ORDER BY order_id DESC;
