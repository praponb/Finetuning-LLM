SELECT txn_time, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label, CAST(is_flagged AS INT) AS is_flagged_flag FROM transactions;
