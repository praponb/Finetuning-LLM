SELECT c.country, SORT_ARRAY(COLLECT_SET(a.account_type)) AS account_type_list
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
WHERE a.customer_id IS NOT NULL
GROUP BY c.country
ORDER BY country
