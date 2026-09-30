SELECT s.region, COUNT(CASE WHEN o.status = 'PENDING' THEN 1 END) pending_count, SORT_ARRAY(COLLECT_SET(o.status)) status_list
FROM orders o
LEFT JOIN stores s ON s.id = o.store_id
GROUP BY s.region
