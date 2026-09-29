SELECT is_flagged, merchant, DATE_FORMAT(txn_time, 'yyyy-MM-dd') txn_time_label
FROM transactions
ORDER BY 2 ASC, txn_time_label ASC;
