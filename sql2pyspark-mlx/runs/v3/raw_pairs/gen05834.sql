SELECT carrier, CASE WHEN YEAR(delivered_date) < 2024 THEN 'before_2024' WHEN YEAR(delivered_date) = 2024 THEN '2024' ELSE 'after_2024' END AS delivered_cohort, COUNT(cost) AS cost_count
FROM shipments
WHERE order_id = 9 AND delivered_date <= '2025-05-29' AND order_id IN (21, 32, 40)
GROUP BY carrier, delivered_cohort
ORDER BY carrier;
