SELECT CASE WHEN s.region IN ('West', 'South') THEN 'tier_1' ELSE 'other' END AS region_group, MAX(p.paid_at) AS latest_paid_at, ROUND(STDDEV(p.amount), 2) AS stddev_amount
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
INNER JOIN stores s ON o.store_id = s.id
WHERE p.order_id IS NOT NULL
GROUP BY CASE WHEN s.region IN ('West', 'South') THEN 'tier_1' ELSE 'other' END;
