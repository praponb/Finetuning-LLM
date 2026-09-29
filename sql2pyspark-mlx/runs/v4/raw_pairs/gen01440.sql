WITH order_stats AS (
    SELECT order_id, COUNT(quantity) AS quantity_count, SUM(unit_price) AS total_unit_price
    FROM order_items
    WHERE quantity BETWEEN 3 AND 4
    GROUP BY order_id
)
SELECT
    o.amount,
    o.discount,
    os.quantity_count,
    os.total_unit_price
FROM orders o
INNER JOIN order_stats os ON o.id = os.order_id
WHERE os.quantity_count > 4
ORDER BY os.quantity_count;
