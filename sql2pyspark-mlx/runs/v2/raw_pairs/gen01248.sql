SELECT
    a.id,
    a.customer_id,
    a.balance,
    a.status
FROM accounts a
WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.account_id IN (18, 26, 37))
ORDER BY 3 DESC;
