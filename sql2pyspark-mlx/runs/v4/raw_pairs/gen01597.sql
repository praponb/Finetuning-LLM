SELECT
    DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month,
    ROUND((100 * SUM(CASE WHEN category = 'Home' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_home,
    MIN(cost) AS min_cost,
    ROUND((100 * SUM(CASE WHEN brand = 'Stark' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_stark
FROM products
GROUP BY 1;
