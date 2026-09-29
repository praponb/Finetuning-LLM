SELECT orders.id, orders.order_date, orders.store_id, orders.shipping_fee FROM orders WHERE orders.store_id NOT IN (SELECT id FROM stores WHERE opened_date >= '2024-06-01') ORDER BY id DESC
