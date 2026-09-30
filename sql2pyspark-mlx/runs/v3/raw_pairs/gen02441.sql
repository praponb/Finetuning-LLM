SELECT category, launch_month, pct_acme
FROM (
    SELECT category, MONTH(launch_date) AS launch_month, ROUND((100 * SUM(CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_acme
    FROM products
    WHERE cost BETWEEN 570 AND 620
    GROUP BY category, MONTH(launch_date)
) AS sub
WHERE pct_acme < 34
ORDER BY pct_acme DESC
