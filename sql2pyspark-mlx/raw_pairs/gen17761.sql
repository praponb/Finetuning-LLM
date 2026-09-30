SELECT id, brand, cost, launch_date FROM (SELECT id, brand, cost, launch_date, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost DESC, id) AS rnk FROM products) AS r WHERE rnk <= 5
