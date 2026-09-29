SELECT o.customer_id, o.store_id, o.shipping_fee, s.sqft FROM orders o LEFT OUTER JOIN stores s ON s.id = o.store_id WHERE s.store_name LIKE '%River%'
