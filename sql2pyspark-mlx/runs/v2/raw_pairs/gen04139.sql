SELECT status, amount, GREATEST(discount, shipping_fee) AS greatest_discount_shipping_fee
FROM orders
ORDER BY amount DESC;
