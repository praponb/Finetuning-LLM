SELECT id, store_id, COALESCE(discount, 0) AS discount_filled
FROM orders
WHERE order_date <= '2025-05-01' AND (shipping_fee BETWEEN 29 AND 31 OR shipping_fee >= 25);
