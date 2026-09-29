SELECT
    a.status,
    a.customer_id,
    SUM(CASE WHEN t.merchant = 'Shell' THEN t.amount ELSE 0 END) shell_amount,
    COUNT(CASE WHEN t.merchant = '7-Eleven' THEN 1 END) 7_eleven_count
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
GROUP BY a.status, a.customer_id
ORDER BY a.customer_id DESC
