SELECT id, brand, launch_date, cost FROM (SELECT id, brand, launch_date, cost, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date, id) AS rnk FROM products) AS t WHERE rnk = 1
