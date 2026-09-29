SELECT channel, ROUND((100 * SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cancelled, ROUND((100 * SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_partner
FROM orders
GROUP BY channel
HAVING ROUND((100 * SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)) / COUNT(*), 1) < 46
ORDER BY pct_partner, channel
LIMIT 5
