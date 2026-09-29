SELECT a.id, COUNT(t.id) AS transaction_count, SUM(CASE WHEN t.merchant = '7-Eleven' THEN t.amount ELSE 0 END) AS 7_eleven_amount
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
GROUP BY a.id;
