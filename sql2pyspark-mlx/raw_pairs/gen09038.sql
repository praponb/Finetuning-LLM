SELECT is_flagged, ROUND(amount, 1) AS amount_rounded
FROM transactions
WHERE amount < 2000 AND NOT is_flagged AND is_flagged;
