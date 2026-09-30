SELECT
    QUARTER(launch_date) launch_quarter,
    SUM(cost) total_cost,
    COUNT(*) cnt,
    DENSE_RANK() OVER (ORDER BY SUM(cost) DESC) AS cost_rank
FROM products
WHERE brand IN ('Initech', 'Umbrella', 'Acme')
GROUP BY QUARTER(launch_date)
ORDER BY 4 DESC;
