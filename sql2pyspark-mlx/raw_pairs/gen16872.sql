SELECT carrier, DATEDIFF(shipped_date, delivered_date) / 7 AS weeks_delivered_date_to_shipped_date
FROM shipments
WHERE shipped_date <= '2025-07-01';
