SELECT customer_id FROM orders WHERE shipping_fee BETWEEN 23 AND 39 UNION SELECT customer_id FROM orders WHERE shipping_fee >= 21;
