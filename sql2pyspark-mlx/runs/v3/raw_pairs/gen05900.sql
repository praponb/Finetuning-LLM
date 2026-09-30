SELECT is_flagged, SUM(amount) AS total_amount
FROM transactions
GROUP BY is_flagged;
