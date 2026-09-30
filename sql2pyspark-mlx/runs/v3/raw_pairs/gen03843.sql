SELECT p.order_id, o.shipping_fee, s.opened_date FROM payments p INNER JOIN orders o ON p.order_id = o.id INNER JOIN stores s ON o.store_id = s.id WHERE s.region = 'West';
