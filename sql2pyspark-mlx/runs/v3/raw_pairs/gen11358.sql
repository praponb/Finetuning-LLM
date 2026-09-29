SELECT store_id, shipping_fee
FROM orders
WHERE discount <= 0.26
ORDER BY 2 DESC;
