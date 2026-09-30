SELECT
    o.id,
    o.channel,
    o.shipping_fee,
    t.status_list,
    t.avg_amount
FROM orders o
LEFT OUTER JOIN (SELECT order_id, SORT_ARRAY(COLLECT_SET(status)) AS status_list, AVG(amount) AS avg_amount
    FROM payments
    WHERE status IN ('SUCCESS', 'REFUNDED')
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.avg_amount >= 750
