SELECT
    id,
    txn_time,
    txn_type,
    is_flagged,
    CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
FROM transactions
WHERE merchant = 'Starbucks' OR NOT is_flagged;
