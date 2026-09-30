SELECT is_flagged, amount, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label FROM transactions WHERE txn_type <> 'DEBIT' AND is_flagged;
