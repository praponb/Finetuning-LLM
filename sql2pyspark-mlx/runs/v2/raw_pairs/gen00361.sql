SELECT
    id,
    delivered_date,
    carrier,
    DATE_SUB(shipped_date, 76) shipped_date_minus_76d
FROM shipments
WHERE carrier <> 'FedEx' AND delivered_date > '2024-12-01';
