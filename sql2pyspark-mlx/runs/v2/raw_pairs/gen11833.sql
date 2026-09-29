SELECT
    id,
    email,
    city,
    LOWER(email) AS email_lower
FROM customers
WHERE credit_limit < 7400 AND age <> 38;
