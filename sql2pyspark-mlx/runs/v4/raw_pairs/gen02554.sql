SELECT customers.is_active, COUNT(DISTINCT accounts.balance) AS unique_statuss, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(accounts.status))) AS status_values
FROM accounts
INNER JOIN customers ON accounts.customer_id = customers.id
GROUP BY customers.is_active
