SELECT DATE_FORMAT(s.opened_date, 'yyyy-MM') opened_month, COUNT(CASE WHEN o.channel = 'store' THEN 1 END) store_count
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
WHERE s.city = 'Singapore'
GROUP BY DATE_FORMAT(s.opened_date, 'yyyy-MM');
