SELECT
    account_type,
    MAX(balance) AS max_balance,
    SUM(CASE WHEN status = 'CLOSED' THEN balance ELSE 0 END) AS closed_balance,
    AVG(balance) AS avg_balance
FROM accounts
WHERE (customer_id IN (2, 5, 14, 39) AND balance < 40000) OR opened_date BETWEEN '2024-12-17' AND '2025-01-27'
GROUP BY account_type
