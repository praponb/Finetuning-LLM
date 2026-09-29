SELECT
    id,
    age,
    email,
    country,
    CASE WHEN credit_limit >= 15100 THEN 'High' WHEN credit_limit >= 4800 THEN 'Medium' ELSE 'Low' END AS credit_limit_tier
FROM customers
ORDER BY email
