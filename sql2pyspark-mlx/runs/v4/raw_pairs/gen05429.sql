SELECT DATE_TRUNC('MONTH', order_date) AS order_month, COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) AS cancelled_count, COUNT(discount) AS discount_count
FROM orders
GROUP BY 1;
