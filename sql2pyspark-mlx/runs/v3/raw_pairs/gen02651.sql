SELECT id, country, city, credit_limit, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY credit_limit DESC, id) AS row_num FROM customers WHERE name LIKE '%bob%'
