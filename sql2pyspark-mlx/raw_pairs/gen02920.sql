SELECT
    a.account_type,
    COUNT(t.id) AS transaction_count,
    ROUND(STDDEV(t.amount), 2) AS stddev_amount,
    AVG(t.amount) AS avg_amount
FROM accounts a
LEFT OUTER JOIN transactions t ON t.account_id = a.id
WHERE a.account_type IN ('SAVINGS', 'CHECKING', 'CREDIT')
GROUP BY a.account_type;
