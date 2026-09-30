SELECT s.region, ROUND(STDDEV(o.amount), 2) AS stddev_amount, ROUND((100 * SUM(CASE WHEN o.channel = 'mobile' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_mobile
FROM orders o
FULL OUTER JOIN stores s ON o.store_id = s.id
WHERE s.opened_date < '2025-07-01'
GROUP BY s.region
HAVING ROUND(STDDEV(o.amount), 2) >= 35
ORDER BY stddev_amount;
