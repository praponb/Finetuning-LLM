SELECT opened_date
FROM accounts
WHERE opened_date <= '2024-04-01'
EXCEPT
SELECT signup_date AS opened_date
FROM customers
