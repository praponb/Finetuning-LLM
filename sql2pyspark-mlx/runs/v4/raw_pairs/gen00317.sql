SELECT launch_month, sports_count
FROM (
    SELECT DATE_TRUNC('MONTH', launch_date) AS launch_month, SUM(CASE WHEN category = 'Sports' THEN 1 ELSE 0 END) AS sports_count
    FROM products
    GROUP BY DATE_TRUNC('MONTH', launch_date)
) AS t
WHERE sports_count >= 73
ORDER BY launch_month DESC
LIMIT 3;
