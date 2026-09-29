WITH order_totals AS (
    SELECT order_id, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM shipments
    WHERE YEAR(shipped_date) >= 2025
    GROUP BY order_id
)
SELECT o.order_date, ot.status_list
FROM orders o
JOIN order_totals ot ON o.id = ot.order_id
ORDER BY o.order_date DESC;
