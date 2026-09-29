SELECT
    account_type,
    status,
    MAX(balance) AS max_balance,
    COUNT(CASE WHEN account_type = 'CHECKING' THEN 1 END) AS checking_count
FROM accounts
WHERE customer_id IS NOT NULL OR account_type IN ('SAVINGS', 'LOAN')
GROUP BY 1, 2
