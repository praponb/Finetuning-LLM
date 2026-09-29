SELECT txn_type, DATE_TRUNC('year', txn_time) AS txn_time_period, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label FROM transactions WHERE txn_type IN ('DEBIT', 'CREDIT', 'REFUND');
