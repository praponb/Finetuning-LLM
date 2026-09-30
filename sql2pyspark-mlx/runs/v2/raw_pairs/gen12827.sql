SELECT
    id,
    delivered_date,
    shipped_date,
    SUBSTRING(carrier, 1, 2) AS carrier_prefix,
    TRIM(carrier) AS clean_carrier
FROM shipments
ORDER BY clean_carrier;
