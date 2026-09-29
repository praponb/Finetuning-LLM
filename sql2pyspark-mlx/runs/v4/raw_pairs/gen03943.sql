SELECT order_id, total_count
FROM (
    SELECT order_id, COUNT(*) AS total_count
    FROM order_items
    WHERE quantity = 8
    GROUP BY order_id
) AS agg
WHERE total_count > 9
