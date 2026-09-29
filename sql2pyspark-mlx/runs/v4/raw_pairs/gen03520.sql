SELECT MONTH(c.signup_date) AS signup_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.status))) AS status_values
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
LEFT JOIN customers c ON o.customer_id = c.id
GROUP BY MONTH(c.signup_date)
ORDER BY status_values ASC, signup_month DESC
