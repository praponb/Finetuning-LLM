SELECT o.shipping_fee, o.channel, t.status_list
FROM orders o
INNER JOIN (SELECT order_id, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM payments
    GROUP BY order_id) t ON o.id = t.order_id
ORDER BY 2;
