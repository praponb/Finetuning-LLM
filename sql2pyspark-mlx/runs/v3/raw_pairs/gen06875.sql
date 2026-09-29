SELECT
    order_id,
    cost,
    carrier,
    LOWER(carrier) AS carrier_lower
FROM shipments
WHERE shipped_date < '2024-04-02';
