SELECT MONTH(order_date) order_month, COUNT(DISTINCT shipping_fee) unique_statuss, MAX(discount) - MIN(discount) discount_range
FROM orders
GROUP BY order_month
ORDER BY unique_statuss DESC, order_month ASC
LIMIT 20;
