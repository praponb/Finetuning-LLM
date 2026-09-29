SELECT CASE WHEN o.channel IN ('store', 'web') THEN 'tier_1' ELSE 'other' END AS channel_group, o.status, MAX(p.amount) - MIN(p.amount) AS amount_range
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
INNER JOIN stores s ON o.store_id = s.id
WHERE p.method IN ('card', 'cash', 'promptpay')
GROUP BY CASE WHEN o.channel IN ('store', 'web') THEN 'tier_1' ELSE 'other' END, o.status;
