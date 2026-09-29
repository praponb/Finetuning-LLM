WITH top_customers AS (SELECT id, city, age, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age DESC, id) AS pos FROM customers) SELECT * FROM top_customers WHERE pos <= 3 ORDER BY city ASC
