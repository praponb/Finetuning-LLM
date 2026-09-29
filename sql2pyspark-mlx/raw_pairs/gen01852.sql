SELECT id, city, age, segment, credit_limit, NTILE(2) OVER (PARTITION BY city ORDER BY age, id) quartile, AVG(credit_limit) OVER (PARTITION BY segment) segment_avg_credit_limit FROM customers
