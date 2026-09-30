SELECT
    segment,
    is_active,
    signup_date,
    credit_limit + age AS credit_limit_plus_age
FROM customers
WHERE LENGTH(email) > 14
ORDER BY segment DESC;
