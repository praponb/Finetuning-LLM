WITH ranked_customers AS (SELECT *, RANK() OVER (PARTITION BY city ORDER BY age DESC) AS rnk FROM customers WHERE age <= 55) SELECT id, city, age, signup_date FROM ranked_customers WHERE rnk <= 5;
