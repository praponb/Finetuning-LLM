SELECT
    id,
    credit_limit,
    signup_date,
    is_active
FROM customers
WHERE credit_limit >= (SELECT AVG(credit_limit) FROM customers);
