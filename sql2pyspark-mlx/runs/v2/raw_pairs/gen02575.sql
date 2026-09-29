SELECT category, YEAR(launch_date) launch_year, MAX(cost) - MIN(cost) cost_range, MIN(cost) min_cost FROM products GROUP BY category, YEAR(launch_date);
