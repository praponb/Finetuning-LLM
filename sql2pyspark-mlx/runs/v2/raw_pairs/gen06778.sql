SELECT
    status,
    account_type,
    SUM(CASE WHEN status = 'FROZEN' THEN balance ELSE 0 END) AS frozen_balance,
    AVG(balance) AS avg_balance
FROM accounts
WHERE customer_id IN (4, 6, 16, 29) AND (balance < 27000 OR customer_id IS NULL)
GROUP BY 1, 2;
