SELECT id, city, age, MIN(age) OVER (PARTITION BY city) AS city_min_age FROM customers;
