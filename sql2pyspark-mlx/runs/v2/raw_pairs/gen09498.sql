SELECT id, city, age, AVG(age) OVER (PARTITION BY city) AS city_avg_age FROM customers;
