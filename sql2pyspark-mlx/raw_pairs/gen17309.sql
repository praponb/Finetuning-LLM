SELECT
    id,
    carrier,
    delivered_date,
    DATEDIFF(delivered_date, shipped_date) / 7 AS weeks_shipped_date_to_delivered_date,
    CONCAT_WS('-', carrier, status) AS carrier_status_key
FROM shipments
WHERE delivered_date <= '2024-10-01' AND cost > 86
ORDER BY carrier_status_key ASC, 4 DESC, id ASC
LIMIT 50
