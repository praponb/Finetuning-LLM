SELECT
    id,
    age,
    signup_date,
    city,
    ROUND(credit_limit * 0.9, 2) credit_limit_adjusted
FROM customers
WHERE segment IN ('SMB', 'Corporate') AND age >= 33;
