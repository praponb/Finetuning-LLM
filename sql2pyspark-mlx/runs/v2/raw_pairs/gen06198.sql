SELECT
    id,
    account_id,
    txn_time,
    merchant,
    CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END is_flagged_label,
    WEEKOFYEAR(txn_time) txn_time_weekofyear
FROM transactions
