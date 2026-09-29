SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY category ORDER BY launch_date) AS row_num FROM products) AS ranked WHERE row_num <= 2;
