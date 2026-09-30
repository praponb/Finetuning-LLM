SELECT *
FROM orders
WHERE shipping_fee < 39
ORDER BY customer_id DESC;
