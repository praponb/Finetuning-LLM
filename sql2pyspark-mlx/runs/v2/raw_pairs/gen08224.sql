SELECT id, store_id, CAST(discount AS INT) AS discount_int FROM orders WHERE discount IS NOT NULL AND (shipping_fee <= 38 OR order_date > '2023-11-01') ORDER BY store_id DESC, id LIMIT 50
