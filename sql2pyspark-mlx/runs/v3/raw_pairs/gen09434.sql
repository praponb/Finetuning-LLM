SELECT id, status, order_date, shipping_fee FROM (SELECT *, RANK() OVER (PARTITION BY status ORDER BY order_date DESC) AS pos FROM orders) AS ranked WHERE pos <= 3 ORDER BY status DESC
