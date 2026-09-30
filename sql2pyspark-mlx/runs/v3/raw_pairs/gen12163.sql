SELECT CASE WHEN YEAR(delivered_date) < 2024 THEN 'before_2024' WHEN YEAR(delivered_date) = 2024 THEN '2024' ELSE 'after_2024' END AS delivered_cohort, COUNT(*) AS num_shipments
FROM shipments
GROUP BY 1
ORDER BY num_shipments ASC, delivered_cohort DESC
