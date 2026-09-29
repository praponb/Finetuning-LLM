SELECT
    shipped_date,
    carrier,
    ADD_MONTHS(delivered_date, 12) AS delivered_date_plus_months,
    CONCAT(carrier, ', ', status) AS carrier_status
FROM shipments
WHERE carrier NOT IN ('Flash', 'DHL')
