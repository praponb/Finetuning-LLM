SELECT
    id,
    txn_type,
    txn_time,
    ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS txn_time_row
FROM transactions
WHERE merchant IN ('7-Eleven', 'Shell', 'Starbucks');
