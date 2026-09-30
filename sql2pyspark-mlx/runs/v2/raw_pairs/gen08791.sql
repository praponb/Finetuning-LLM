SELECT is_flagged, ROUND(AVG(amount), 2) AS avg_amount, MAX(amount) AS max_amount
FROM transactions
WHERE NOT is_flagged AND merchant = 'Shell'
GROUP BY is_flagged
HAVING MAX(amount) < 3800
