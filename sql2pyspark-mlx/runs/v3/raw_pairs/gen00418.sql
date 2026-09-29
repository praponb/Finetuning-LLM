WITH order_totals AS (
    SELECT order_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM order_items
    WHERE quantity != 6
    GROUP BY order_id
)
SELECT o.status, o.amount, ot.stddev_quantity
FROM orders o
JOIN order_totals ot ON o.id = ot.order_id
WHERE ot.stddev_quantity > 77
ORDER BY status DESC;
