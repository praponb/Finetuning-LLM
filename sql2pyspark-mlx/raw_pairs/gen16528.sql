SELECT id, credit_limit, city, FIRST_VALUE(city) OVER (ORDER BY credit_limit DESC, id) AS first_city FROM customers ORDER BY id DESC, first_city;
