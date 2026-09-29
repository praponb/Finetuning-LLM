SELECT id, channel
FROM promotions
EXCEPT
SELECT id, channel
FROM promotions
WHERE discount_pct BETWEEN 0.22 AND 0.26;
