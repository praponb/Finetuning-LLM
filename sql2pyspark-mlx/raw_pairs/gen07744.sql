SELECT
    delivered_date,
    cost,
    carrier,
    UPPER(status) AS status_upper
FROM shipments
WHERE cost > 87 AND delivered_date < '2024-03-01' AND MONTH(delivered_date) IN (3, 6, 10);
