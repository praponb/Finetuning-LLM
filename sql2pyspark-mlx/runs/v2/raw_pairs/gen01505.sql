WITH order_totals AS (
    SELECT order_id, COUNT(unit_price) AS unit_price_count, SUM(quantity) AS total_quantity
    FROM order_items
    GROUP BY order_id
)
SELECT
    o.amount,
    o.status,
    ot.unit_price_count,
    ot.total_quantity
FROM orders o
JOIN order_totals ot ON o.id = ot.order_id
WHERE ot.total_quantity >= 41
ORDER BY amount
