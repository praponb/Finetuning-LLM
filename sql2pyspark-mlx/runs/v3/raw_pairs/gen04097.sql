SELECT customers.age, orders.order_date, ROUND(orders.discount, 1) AS discount_rounded
FROM customers
INNER JOIN orders ON orders.customer_id = customers.id
WHERE orders.shipping_fee BETWEEN 14 AND 18
ORDER BY discount_rounded DESC
