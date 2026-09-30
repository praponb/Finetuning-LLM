SELECT MIN(opened_date) min_opened_date, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) account_type_values, ROUND(STDDEV(balance), 2) stddev_balance
FROM accounts
WHERE account_type NOT IN ('CREDIT', 'SAVINGS') AND (YEAR(opened_date) = 2023 OR balance BETWEEN 47000 AND 69000)
ORDER BY account_type_values
