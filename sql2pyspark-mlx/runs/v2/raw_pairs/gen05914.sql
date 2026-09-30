SELECT
    a.account_type,
    COUNT(t.id) transaction_count,
    MAX(t.amount) max_amount,
    ROUND(STDDEV(t.amount), 2) stddev_amount
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE t.amount BETWEEN 1200 AND 1400
GROUP BY a.account_type
ORDER BY max_amount
