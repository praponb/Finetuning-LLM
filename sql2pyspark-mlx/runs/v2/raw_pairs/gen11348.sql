SELECT
    age,
    credit_limit,
    name,
    credit_limit + age AS credit_limit_plus_age,
    CAST(credit_limit AS INT) AS credit_limit_int
FROM customers;
