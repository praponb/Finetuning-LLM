SELECT *, discount + shipping_fee AS discount_plus_shipping_fee
FROM orders
ORDER BY id
