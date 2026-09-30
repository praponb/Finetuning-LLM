SELECT id, country, city, credit_limit, DENSE_RANK() OVER (PARTITION BY country, city ORDER BY credit_limit DESC) AS dense_rnk FROM customers WHERE signup_date <= '2025-05-30'
