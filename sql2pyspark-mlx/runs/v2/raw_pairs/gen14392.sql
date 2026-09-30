SELECT
    is_flagged,
    txn_type,
    txn_time,
    CASE WHEN merchant = '7-Eleven' THEN 1 ELSE 0 END AS is_7_eleven,
    CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
FROM transactions
