SELECT
    id,
    status,
    delivered_date,
    carrier,
    FIRST_VALUE(carrier) OVER (PARTITION BY status ORDER BY delivered_date, id) AS first_carrier
FROM shipments;
