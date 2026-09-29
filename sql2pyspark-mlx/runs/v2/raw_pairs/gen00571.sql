SELECT
    carrier,
    delivered_date,
    cost,
    INITCAP(TRIM(carrier)) carrier_title
FROM shipments
