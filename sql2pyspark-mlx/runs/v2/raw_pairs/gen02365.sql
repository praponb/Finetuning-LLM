WITH OrderStats AS (
    SELECT order_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range
    FROM order_items
    GROUP BY order_id
)
SELECT o.amount, O.unit_price_range
FROM orders o
JOIN OrderStats O ON o.id = O.order_id
WHERE O.unit_price_range > 47;
