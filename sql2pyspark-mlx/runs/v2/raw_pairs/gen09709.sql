WITH order_totals AS (
    SELECT order_id, SUM(unit_price) AS total_unit_price, SUM(unit_price * quantity) AS revenue
    FROM order_items
    GROUP BY order_id
)
SELECT o.amount, ot.total_unit_price, ot.revenue
FROM orders o
INNER JOIN order_totals ot ON o.id = ot.order_id
WHERE ot.total_unit_price > 1900;
