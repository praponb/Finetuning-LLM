SELECT *, UPPER(country) AS country_upper, CASE WHEN credit_limit >= 18000 THEN 'High' WHEN credit_limit >= 6500 THEN 'Medium' ELSE 'Low' END AS credit_limit_tier
FROM customers
WHERE email IS NOT NULL AND credit_limit <= 4900;
