SELECT id, country, age FROM (SELECT *, RANK() OVER (PARTITION BY country ORDER BY age DESC) AS row_num FROM customers) AS t WHERE row_num <= 3;
