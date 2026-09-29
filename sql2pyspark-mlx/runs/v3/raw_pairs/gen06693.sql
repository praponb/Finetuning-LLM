SELECT
    category,
    MONTH(launch_date) AS launch_month,
    COUNT(cost) AS cost_count,
    MAX(launch_date) AS last_launch_date,
    MIN(launch_date) AS min_launch_date
FROM products
WHERE cost >= 410
GROUP BY category, launch_month;
