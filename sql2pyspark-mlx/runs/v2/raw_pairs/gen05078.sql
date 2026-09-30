SELECT o.id, o.amount, s.city FROM orders o INNER JOIN stores s ON o.store_id = s.id WHERE MONTH(o.order_date) IN (2, 8, 12) ORDER BY id DESC;
