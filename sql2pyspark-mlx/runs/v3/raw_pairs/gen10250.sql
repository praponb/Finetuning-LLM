SELECT *, ROUND(amount, 2) AS amount_rounded, IF(is_flagged, 'Yes', 'No') AS is_flagged_label
FROM transactions
WHERE account_id IN (13, 18, 24);
