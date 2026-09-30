SELECT YEAR(a.opened_date) AS opened_year, COUNT(*) AS num_transactions, SORT_ARRAY(COLLECT_SET(t.txn_type)) AS txn_type_list
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
WHERE t.merchant != '7-Eleven'
GROUP BY YEAR(a.opened_date)
ORDER BY opened_year
