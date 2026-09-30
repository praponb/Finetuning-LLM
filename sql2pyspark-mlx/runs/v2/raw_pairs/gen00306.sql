SELECT DATE_FORMAT(a.opened_date, 'yyyy-MM') AS opened_month, CASE WHEN a.balance >= 75000 THEN 'high' WHEN a.balance >= 36000 THEN 'mid' ELSE 'low' END AS balance_bucket, SORT_ARRAY(COLLECT_SET(t.merchant)) AS merchant_list
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON c.id = a.customer_id
GROUP BY DATE_FORMAT(a.opened_date, 'yyyy-MM'), CASE WHEN a.balance >= 75000 THEN 'high' WHEN a.balance >= 36000 THEN 'mid' ELSE 'low' END
ORDER BY opened_month
