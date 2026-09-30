SELECT id, store_id, shipping_fee, order_date FROM orders WHERE channel IS NOT NULL ORDER BY store_id DESC, id LIMIT 3;
