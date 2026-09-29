SELECT *, amount + shipping_fee AS amount_with_shipping FROM orders WHERE shipping_fee IS NOT NULL ORDER BY amount_with_shipping DESC;
