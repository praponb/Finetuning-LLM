SELECT delivered_date, DATEDIFF(delivered_date, shipped_date) days_shipped_date_to_delivered_date
FROM shipments
WHERE order_id IS NULL
ORDER BY days_shipped_date_to_delivered_date DESC;
