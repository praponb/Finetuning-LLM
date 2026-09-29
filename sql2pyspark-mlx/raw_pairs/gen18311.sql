SELECT
    o.status,
    o.channel,
    ROUND(STDDEV(p.amount), 2) AS stddev_amount,
    SORT_ARRAY(COLLECT_SET(p.method)) AS method_list
FROM payments p
JOIN orders o ON o.id = p.order_id
GROUP BY o.status, o.channel
