SELECT
    carrier,
    MONTH(shipped_date) AS shipped_month,
    MAX(cost) - MIN(cost) AS cost_range,
    ROUND((100 * SUM(CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_fedex
FROM shipments
WHERE YEAR(shipped_date) >= 2025 AND delivered_date BETWEEN '2024-05-21' AND '2025-02-01' AND shipped_date < '2025-08-01'
GROUP BY carrier, MONTH(shipped_date);
