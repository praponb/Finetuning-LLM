SELECT is_flagged, SUM(amount) AS total_amount, SUM(CASE WHEN merchant = 'Starbucks' THEN amount ELSE 0 END) AS starbucks_amount
FROM transactions
WHERE amount <= 2200
GROUP BY is_flagged
HAVING total_amount >= 16000
ORDER BY total_amount DESC, is_flagged
LIMIT 50;
