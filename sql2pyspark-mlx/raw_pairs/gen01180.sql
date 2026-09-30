SELECT CASE WHEN s.city IN ('Tokyo', 'Bangkok') THEN 'group_a' ELSE 'other' END AS city_group, SUM(p.amount) AS total_amount
FROM payments p
RIGHT OUTER JOIN orders o ON o.id = p.order_id
JOIN stores s ON o.store_id = s.id
WHERE s.city NOT IN ('Singapore', 'London')
GROUP BY CASE WHEN s.city IN ('Tokyo', 'Bangkok') THEN 'group_a' ELSE 'other' END
