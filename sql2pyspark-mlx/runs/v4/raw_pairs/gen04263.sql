SELECT order_id, avg_quantity
FROM (
    SELECT order_id, ROUND(AVG(quantity), 2) avg_quantity
    FROM order_items
    GROUP BY order_id
) agg
WHERE avg_quantity > 23
ORDER BY order_id DESC
LIMIT 20;
