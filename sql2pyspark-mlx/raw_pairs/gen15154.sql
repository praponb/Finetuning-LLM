SELECT is_flagged, account_id, CONCAT(txn_type, ' - ', merchant) AS txn_type_merchant
FROM transactions
WHERE NOT is_flagged
