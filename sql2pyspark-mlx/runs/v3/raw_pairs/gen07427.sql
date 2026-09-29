SELECT
    o.id,
    o.shipping_fee,
    o.amount,
    o.customer_id
FROM orders o
WHERE o.channel IN ('mobile', 'web', 'partner') AND o.store_id IN (SELECT id FROM stores WHERE store_name IS NOT NULL)
