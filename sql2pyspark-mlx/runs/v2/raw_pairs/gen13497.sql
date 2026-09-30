SELECT brand, min_cost, min_launch_date FROM (SELECT brand, MIN(cost) AS min_cost, MIN(launch_date) AS min_launch_date FROM products GROUP BY brand) AS x WHERE min_cost >= 220;
