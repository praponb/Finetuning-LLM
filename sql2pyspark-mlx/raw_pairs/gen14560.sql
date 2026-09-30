SELECT id, txn_time, merchant, is_flagged, IF(is_flagged, 'Yes', 'No') AS is_flagged_label FROM transactions WHERE txn_time < '2024-04-10'
