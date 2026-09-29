SELECT o.channel, SORT_ARRAY(COLLECT_SET(p.method)) AS method_list, AVG(p.amount) AS avg_amount
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
WHERE o.customer_id = 34
GROUP BY o.channel
