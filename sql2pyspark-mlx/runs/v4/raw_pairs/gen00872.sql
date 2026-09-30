SELECT DATE_TRUNC('MONTH', order_date) AS order_month, MIN(shipping_fee) AS min_shipping_fee FROM orders GROUP BY DATE_TRUNC('MONTH', order_date);
