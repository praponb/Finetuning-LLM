SELECT c.country, ROUND(AVG(t.amount), 2) AS avg_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(t.merchant))) AS merchant_values
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.customer_id IS NOT NULL
GROUP BY c.country
ORDER BY country
LIMIT 20;
