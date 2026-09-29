SELECT txn_time, CONCAT_WS(',', merchant, txn_type) AS merchant_txn_type_key FROM transactions ORDER BY 1 DESC;
