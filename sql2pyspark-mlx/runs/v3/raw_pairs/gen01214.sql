SELECT id, customer_id, COALESCE(shipping_fee, 0) AS shipping_fee_filled
FROM orders
WHERE shipping_fee > 34 AND store_id IS NOT NULL
ORDER BY shipping_fee_filled, customer_id, id
LIMIT 20;
