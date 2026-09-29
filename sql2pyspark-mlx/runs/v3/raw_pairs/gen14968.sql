SELECT id, city, age, MAX(age) OVER (PARTITION BY city) city_max_age FROM customers
