SELECT id, country, segment, age, ROW_NUMBER() OVER (PARTITION BY country, segment ORDER BY age, id) AS rn FROM customers WHERE LOWER(TRIM(name)) = 'david lee'
