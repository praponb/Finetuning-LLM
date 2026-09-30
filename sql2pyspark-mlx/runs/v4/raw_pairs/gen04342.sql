SELECT
    id,
    shipped_date,
    order_id,
    CONCAT(carrier, ' - ', status) AS carrier_status
FROM shipments
WHERE cost < 89
