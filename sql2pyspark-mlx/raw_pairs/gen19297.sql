SELECT CASE WHEN s.city IN ('Tokyo', 'New York') THEN 'tier_1' ELSE 'other' END AS city_group, MAX(o.amount) AS max_amount, COUNT(o.amount) AS amount_count
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
GROUP BY CASE WHEN s.city IN ('Tokyo', 'New York') THEN 'tier_1' ELSE 'other' END
ORDER BY city_group, amount_count DESC
LIMIT 20
