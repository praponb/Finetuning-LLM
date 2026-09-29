SELECT id, txn_time, IF(is_flagged, 'Yes', 'No') AS is_flagged_label
FROM transactions;
