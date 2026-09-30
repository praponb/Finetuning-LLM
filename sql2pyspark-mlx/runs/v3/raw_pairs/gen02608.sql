SELECT *, amount + shipping_fee AS amount_with_shipping
FROM orders
WHERE customer_id IS NOT NULL AND (shipping_fee BETWEEN 27 AND 36 OR shipping_fee >= 39);
