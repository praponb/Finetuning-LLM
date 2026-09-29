SELECT id, account_id, IF(is_flagged, 'Yes', 'No') AS is_flagged_label
FROM transactions
WHERE (account_id = 20 AND merchant IS NOT NULL) OR amount < 2700
ORDER BY account_id ASC;
