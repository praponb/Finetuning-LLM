SELECT id, city, age, credit_limit, FIRST_VALUE(credit_limit) OVER (PARTITION BY city ORDER BY age DESC, id) first_credit_limit FROM customers WHERE is_active ORDER BY first_credit_limit, id DESC
