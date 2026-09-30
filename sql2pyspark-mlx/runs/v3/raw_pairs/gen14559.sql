SELECT o.id, o.customer_id, o.discount, o.shipping_fee FROM orders o WHERE o.store_id NOT IN (SELECT id FROM stores WHERE sqft IS NOT NULL)
