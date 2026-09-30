SELECT account_type, COUNT(balance) AS balance_count
FROM accounts
WHERE opened_date BETWEEN '2023-07-01' AND '2025-09-01'
GROUP BY account_type
ORDER BY balance_count DESC;
