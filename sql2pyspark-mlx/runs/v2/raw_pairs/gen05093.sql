SELECT channel, AVG(discount) AS avg_discount, ROUND((100 * SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_partner
FROM orders
WHERE amount IS NULL
GROUP BY channel;
