SELECT
    carrier,
    DATE_FORMAT(delivered_date, 'yyyy-MM') AS delivered_month,
    SUM(CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END) AS kerry_count,
    ROUND(STDDEV(cost), 2) AS stddev_cost
FROM shipments
WHERE YEAR(shipped_date) >= 2024
GROUP BY carrier, DATE_FORMAT(delivered_date, 'yyyy-MM');
