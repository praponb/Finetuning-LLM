WITH top_customers AS (SELECT id, country, age, RANK() OVER (PARTITION BY country ORDER BY age DESC) AS pos FROM customers) SELECT id, country, age FROM top_customers WHERE pos <= 3
