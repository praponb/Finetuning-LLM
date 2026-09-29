SELECT MIN(opened_date) AS min_opened_date, SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list
FROM accounts
WHERE NOT (opened_date >= '2023-06-21' AND account_type <> 'LOAN' AND MONTH(opened_date) IN (3, 4, 8))
ORDER BY min_opened_date
