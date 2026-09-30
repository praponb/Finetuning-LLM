SELECT id, shipping_fee, channel
FROM orders
WHERE discount <= 0.11 OR order_date BETWEEN '2023-05-30' AND '2023-11-19';
