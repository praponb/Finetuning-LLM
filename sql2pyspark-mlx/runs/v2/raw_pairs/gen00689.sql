SELECT id, country, segment, age, SUM(age) OVER (PARTITION BY country, segment) AS country_sum_age FROM customers WHERE country NOT IN ('TH', 'DE', 'UK')
