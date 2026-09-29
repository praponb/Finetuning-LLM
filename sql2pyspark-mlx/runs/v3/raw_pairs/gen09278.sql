SELECT status
FROM accounts
UNION
SELECT status
FROM subscriptions
WHERE YEAR(start_date) >= 2024;
