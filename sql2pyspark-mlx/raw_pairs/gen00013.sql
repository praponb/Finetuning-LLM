SELECT *
FROM (
    SELECT order_id, SUM(quantity) AS total_quantity
    FROM order_items
    GROUP BY order_id
) AS x
WHERE total_quantity < 41;
