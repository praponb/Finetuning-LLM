SELECT
    o.id,
    o.shipping_fee,
    o.amount,
    o.discount
FROM orders o
WHERE o.store_id NOT IN (SELECT id FROM stores WHERE region NOT IN ('East', 'Central', 'North'));
