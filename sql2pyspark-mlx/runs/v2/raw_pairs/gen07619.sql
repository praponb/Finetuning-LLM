SELECT
    c.id,
    c.credit_limit,
    c.age,
    t.device_list,
    t.desktop_count
FROM customers c
INNER JOIN (SELECT customer_id, SORT_ARRAY(COLLECT_SET(device)) AS device_list, SUM(CASE WHEN device = 'desktop' THEN 1 ELSE 0 END) AS desktop_count
    FROM sessions
    WHERE browser = 'Chrome'
    GROUP BY customer_id) t ON c.id = t.customer_id;
