SELECT
    id,
    txn_time,
    amount,
    IF(is_flagged, 'Yes', 'No') AS is_flagged_label
FROM transactions
WHERE (amount BETWEEN 1200 AND 3000 AND TO_DATE(txn_time) >= '2023-11-01') OR txn_type != 'DEBIT'
ORDER BY id DESC, is_flagged_label;
