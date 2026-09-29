SELECT id, shipping_fee, amount, order_date, CONCAT(status, '/', channel) AS status_channel FROM orders;
