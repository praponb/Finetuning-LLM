SELECT s.region, MAX(o.order_date) latest_order_date, MAX(o.order_date) last_order_date
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
WHERE LOWER(s.store_name) LIKE '%ld %'
GROUP BY s.region
ORDER BY latest_order_date ASC, region ASC
LIMIT 5;
