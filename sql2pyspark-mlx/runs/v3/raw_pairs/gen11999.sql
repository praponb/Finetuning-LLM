SELECT store_id, shipping_fee, CASE WHEN channel = 'store' THEN 1 ELSE 0 END is_store
FROM orders
WHERE channel IN ('store', 'mobile') OR status = 'COMPLETED'
