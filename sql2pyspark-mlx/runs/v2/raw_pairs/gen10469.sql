SELECT txn_type, merchant, IF(is_flagged, 'Yes', 'No') is_flagged_label
FROM transactions
ORDER BY merchant ASC, is_flagged_label DESC;
