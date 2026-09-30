SELECT
    o.id,
    o.customer_id,
    o.order_date,
    o.shipping_fee
FROM orders o
WHERE o.store_id NOT IN (SELECT id FROM stores WHERE LOWER(TRIM(store_name)) = 'siam flagship')
ORDER BY o.order_date DESC;
