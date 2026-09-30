SELECT QUARTER(launch_date) launch_quarter, MAX(cost) max_cost
FROM products
GROUP BY launch_quarter
ORDER BY max_cost DESC, launch_quarter
LIMIT 50;
