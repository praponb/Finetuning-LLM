SELECT id, brand, launch_date FROM (SELECT *, RANK() OVER (PARTITION BY brand ORDER BY launch_date DESC) AS row_num FROM products) AS r WHERE row_num <= 5 ORDER BY launch_date, brand;
