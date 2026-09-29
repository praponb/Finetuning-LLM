SELECT *, DATE_SUB(shipped_date, 25) AS shipped_date_minus_25d, TRIM(carrier) AS clean_carrier
FROM shipments
