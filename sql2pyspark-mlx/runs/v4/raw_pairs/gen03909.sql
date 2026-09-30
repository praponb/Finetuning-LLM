SELECT DATE_TRUNC('MONTH', c.signup_date) signup_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(t.merchant))) merchant_values, ROUND(STDDEV(t.amount), 2) stddev_amount
FROM transactions t
JOIN accounts a ON t.account_id = a.id
LEFT JOIN customers c ON a.customer_id = c.id
WHERE t.txn_type = 'REFUND'
GROUP BY DATE_TRUNC('MONTH', c.signup_date)
ORDER BY merchant_values, signup_month
LIMIT 5;
