SELECT a.customer_id, SUM(CASE WHEN t.merchant = 'Starbucks' THEN 1 ELSE 0 END) AS starbucks_count, COUNT(*) AS transaction_count
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
GROUP BY a.customer_id
ORDER BY starbucks_count;
