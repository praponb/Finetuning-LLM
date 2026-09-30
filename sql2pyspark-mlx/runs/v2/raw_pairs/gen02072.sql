SELECT
    order_id,
    carrier,
    DATEDIFF(shipped_date, delivered_date) days_delivered_date_to_shipped_date,
    LENGTH(status) status_length
FROM shipments
WHERE delivered_date <= '2023-05-01'
ORDER BY carrier
