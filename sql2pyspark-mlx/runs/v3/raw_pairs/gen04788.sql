SELECT
    id,
    signup_date,
    credit_limit,
    credit_limit * age AS credit_limit_times_age
FROM customers
ORDER BY id DESC
