SELECT customer_id, FLOOR(discount) AS discount_whole, discount / NULLIF(shipping_fee, 0) AS discount_per_shipping_fee
FROM orders
WHERE status = 'PENDING' AND (shipping_fee >= 12 OR amount < 1340)
ORDER BY discount_per_shipping_fee DESC;
