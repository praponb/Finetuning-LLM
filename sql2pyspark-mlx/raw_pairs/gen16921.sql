SELECT ROUND((100 * SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_partner, MAX(amount) AS max_amount FROM orders ORDER BY pct_partner DESC;
