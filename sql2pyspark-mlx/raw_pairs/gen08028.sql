WITH recent_order_items AS (
    SELECT *, unit_price * quantity AS line_total
    FROM order_items
    WHERE quantity < 7 AND quantity != 7
),
order_items_stats AS (
    SELECT order_id, MAX(line_total) - MIN(line_total) AS line_total_range, ROUND(AVG(unit_price), 2) AS avg_unit_price
    FROM recent_order_items
    GROUP BY order_id
)
SELECT *
FROM order_items_stats
WHERE avg_unit_price >= 72
