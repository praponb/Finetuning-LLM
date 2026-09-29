SELECT id, txn_type, merchant, txn_time, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label, ROUND(amount * 1.15, 2) AS amount_adjusted FROM transactions WHERE account_id = 23
