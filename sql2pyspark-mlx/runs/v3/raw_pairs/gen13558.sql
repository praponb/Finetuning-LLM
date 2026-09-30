SELECT account_type, opened_date
FROM accounts
WHERE opened_date BETWEEN '2023-03-07' AND '2025-07-01'
UNION ALL
SELECT account_type, opened_date
FROM accounts
WHERE balance < 78000
ORDER BY opened_date
