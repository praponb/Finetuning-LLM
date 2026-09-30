SELECT c.id, c.segment, t.status_list
FROM customers c
LEFT JOIN (SELECT customer_id, SORT_ARRAY(COLLECT_SET(status)) status_list
    FROM orders
    WHERE YEAR(order_date) >= 2023
    GROUP BY customer_id) t ON c.id = t.customer_id
ORDER BY c.segment, c.id DESC;
