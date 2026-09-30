SELECT *
FROM (
    SELECT account_id, ROUND(STDDEV(amount), 2) AS stddev_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
    FROM transactions
    WHERE is_flagged = TRUE
    GROUP BY account_id
) AS t
WHERE stddev_amount >= 24
