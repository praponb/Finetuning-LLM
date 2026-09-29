SELECT
    status,
    carrier,
    shipped_date,
    ROUND(cost, 1) AS cost_rounded
FROM shipments
WHERE cost >= 66 OR shipped_date < '2023-06-01';
