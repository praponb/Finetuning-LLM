SELECT DATE_TRUNC('MONTH', o.order_date) AS order_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.status))) AS status_values, AVG(p.amount) AS avg_amount
FROM payments p
FULL OUTER JOIN orders o ON p.order_id = o.id
LEFT JOIN stores s ON o.store_id = s.id
GROUP BY DATE_TRUNC('MONTH', o.order_date)
ORDER BY order_month;
