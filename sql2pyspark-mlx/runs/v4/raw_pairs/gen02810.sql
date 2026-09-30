SELECT carrier, ROUND((100 * SUM(CASE WHEN carrier = 'Flash' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_flash
FROM shipments
GROUP BY carrier
HAVING ROUND((100 * SUM(CASE WHEN carrier = 'Flash' THEN 1 ELSE 0 END)) / COUNT(*), 1) > 65
ORDER BY carrier ASC
LIMIT 20
