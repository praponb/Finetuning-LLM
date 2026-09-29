SELECT id, merchant, amount, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label, SUBSTRING(txn_type, 1, 4) AS txn_type_prefix FROM transactions WHERE is_flagged = TRUE
