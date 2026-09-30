SELECT o.id, o.status, o.amount, o.shipping_fee, s.city FROM orders o LEFT OUTER JOIN stores s ON o.store_id = s.id ORDER BY o.shipping_fee ASC
