SELECT
    DATE_TRUNC('MONTH', shipped_date) AS shipped_month,
    carrier,
    COUNT(CASE WHEN carrier = 'FedEx' THEN 1 END) AS fedex_count,
    SUM(cost) AS total_cost
FROM shipments
WHERE cost < 87
GROUP BY 1, 2
ORDER BY fedex_count DESC
