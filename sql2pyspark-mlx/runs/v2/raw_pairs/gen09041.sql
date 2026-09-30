SELECT
    txn_type,
    YEAR(txn_time) txn_year,
    SORT_ARRAY(COLLECT_SET(txn_type)) txn_type_list,
    MAX(amount) max_amount
FROM transactions
GROUP BY 1, 2
