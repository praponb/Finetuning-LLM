SELECT id, is_flagged, account_id
FROM transactions
WHERE amount <= 3100 AND amount >= 3800;
