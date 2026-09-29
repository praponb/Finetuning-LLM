SELECT txn_type, SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list, MIN(amount) AS min_amount FROM transactions WHERE merchant = 'Grab' GROUP BY txn_type HAVING COUNT(DISTINCT amount) > 1;
