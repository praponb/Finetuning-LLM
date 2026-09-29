WITH src AS (
    SELECT *, CEIL(cost) AS cost_whole
    FROM products
    WHERE NOT (supplier_id = 21 AND launch_date <= '2025-09-01')
),
launch_month_summary AS (
    SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, COUNT(cost_whole) AS cost_whole_count
    FROM src
    GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM')
)
SELECT launch_month, cost_whole_count
FROM launch_month_summary
ORDER BY cost_whole_count
