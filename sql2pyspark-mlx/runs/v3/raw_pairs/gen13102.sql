SELECT account_type, status, opened_date
FROM accounts
WHERE opened_date BETWEEN '2023-07-01' AND '2024-07-01'
ORDER BY account_type ASC;
