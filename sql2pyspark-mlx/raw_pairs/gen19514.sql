SELECT s.store_name, s.opened_date, o.amount, o.status, o.order_date FROM stores s INNER JOIN orders o ON o.store_id = s.id ORDER BY amount DESC;
