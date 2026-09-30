SELECT
    status,
    DATE_FORMAT(delivered_date, 'yyyy-MM') AS delivered_month,
    MIN(cost) AS min_cost,
    MIN(shipped_date) AS first_shipped_date
FROM shipments
WHERE cost >= 88
GROUP BY status, DATE_FORMAT(delivered_date, 'yyyy-MM');
