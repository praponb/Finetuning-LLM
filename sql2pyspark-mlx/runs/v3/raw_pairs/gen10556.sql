SELECT CASE WHEN o.channel IN ('mobile', 'partner') THEN 'tier_1' ELSE 'other' END AS channel_group, SUM(CASE WHEN p.status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count, COUNT(*) AS cnt
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
GROUP BY CASE WHEN o.channel IN ('mobile', 'partner') THEN 'tier_1' ELSE 'other' END
ORDER BY success_count DESC;
