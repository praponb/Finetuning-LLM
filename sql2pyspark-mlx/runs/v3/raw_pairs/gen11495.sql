SELECT id, amount, CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END AS is_shipped FROM orders WHERE shipping_fee >= 10
