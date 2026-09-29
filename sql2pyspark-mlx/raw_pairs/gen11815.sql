SELECT
    id,
    delivered_date,
    carrier,
    cost,
    DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date
FROM shipments
WHERE status <> 'IN_TRANSIT';
