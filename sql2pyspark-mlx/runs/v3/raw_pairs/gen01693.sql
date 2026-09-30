SELECT DATE_FORMAT(c.signup_date, 'yyyy-MM') AS signup_month, MAX(o.order_date) AS latest_order_date, SUM(o.shipping_fee) AS total_shipping_fee
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY DATE_FORMAT(c.signup_date, 'yyyy-MM');
