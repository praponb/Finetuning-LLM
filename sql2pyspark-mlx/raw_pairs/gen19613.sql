SELECT
    a.id,
    COUNT(t.id) AS transaction_count,
    SUM(CASE WHEN t.merchant = 'Grab' THEN t.amount ELSE 0 END) AS grab_amount,
    ROUND(STDDEV(t.amount), 2) AS stddev_amount
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
GROUP BY a.id
