SELECT
    shipped_date,
    order_id,
    cost,
    CONCAT_WS('-', carrier, status) AS carrier_status_key,
    DATE_TRUNC('week', shipped_date) AS shipped_date_period
FROM shipments
WHERE delivered_date BETWEEN '2025-01-01' AND '2025-02-01'
ORDER BY cost;
