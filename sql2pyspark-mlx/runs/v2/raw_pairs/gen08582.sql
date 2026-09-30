SELECT
    id,
    cost,
    carrier,
    shipped_date,
    DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date
FROM shipments
WHERE (order_id = 9 AND order_id IN (7, 25, 30)) OR status = 'LOST'
ORDER BY shipped_date DESC, id
LIMIT 10;
