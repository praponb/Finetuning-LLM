SELECT opened_date, customer_id
FROM accounts
WHERE YEAR(opened_date) >= 2025
UNION ALL
SELECT opened_date, customer_id
FROM accounts
WHERE opened_date <= '2024-03-01'
