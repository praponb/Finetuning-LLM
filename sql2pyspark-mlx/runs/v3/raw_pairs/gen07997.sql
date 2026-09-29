SELECT
    id,
    delivered_date,
    cost,
    CONCAT_WS(',', carrier, status) AS carrier_status_key
FROM shipments
WHERE shipped_date BETWEEN '2023-04-21' AND '2025-06-01'
ORDER BY cost
