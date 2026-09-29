SELECT o.store_id, ROUND(STDDEV(p.amount), 2) AS stddev_amount, ROUND((100 * SUM(CASE WHEN p.status = 'SUCCESS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_success
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
GROUP BY o.store_id
HAVING stddev_amount > 39;
