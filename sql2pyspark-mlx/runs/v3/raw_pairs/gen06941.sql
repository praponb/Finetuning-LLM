SELECT id, account_id, IF(is_flagged, 'Yes', 'No') AS is_flagged_label
FROM transactions
WHERE txn_type = 'FEE' AND is_flagged = TRUE;
