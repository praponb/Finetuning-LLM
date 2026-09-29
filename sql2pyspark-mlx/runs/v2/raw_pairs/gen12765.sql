SELECT *, LEAST(shipping_fee, discount) AS least_shipping_fee_discount, shipping_fee * amount AS amount_with_shipping FROM orders WHERE shipping_fee > 22 ORDER BY status;
