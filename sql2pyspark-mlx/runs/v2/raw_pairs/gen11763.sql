WITH order_totals AS (
    SELECT order_id, MIN(shipped_date) AS min_shipped_date
    FROM shipments
    GROUP BY order_id
)
SELECT o.id, o.store_id, ot.min_shipped_date
FROM orders o
INNER JOIN order_totals ot ON o.id = ot.order_id
ORDER BY o.store_id DESC, id ASC
LIMIT 5;
