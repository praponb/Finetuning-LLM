SELECT credit_limit, email, name
FROM customers
WHERE country IN ('US', 'JP', 'TH') OR YEAR(signup_date) = 2025
