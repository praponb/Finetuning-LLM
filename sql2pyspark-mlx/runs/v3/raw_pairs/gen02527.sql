SELECT o.id, o.channel, t.method_list
FROM orders o
LEFT JOIN (SELECT order_id, SORT_ARRAY(COLLECT_SET(method)) AS method_list
    FROM payments
    WHERE amount > 1210
    GROUP BY order_id) t ON o.id = t.order_id
ORDER BY o.channel ASC;
