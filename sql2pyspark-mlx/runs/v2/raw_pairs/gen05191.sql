SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS order_month, SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM orders
GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
HAVING COUNT(shipping_fee) >= 5
ORDER BY order_month
LIMIT 3;
