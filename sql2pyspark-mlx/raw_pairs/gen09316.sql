SELECT a.status, MIN(t.amount) AS min_amount, SORT_ARRAY(COLLECT_SET(t.merchant)) AS merchant_list
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
GROUP BY a.status
ORDER BY min_amount
