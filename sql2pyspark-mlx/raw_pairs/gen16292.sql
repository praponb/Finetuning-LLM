SELECT DATE_TRUNC('MONTH', order_date) order_month, MIN(amount) min_amount
FROM orders
GROUP BY 1;
