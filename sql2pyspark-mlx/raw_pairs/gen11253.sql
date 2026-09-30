SELECT
    id,
    shipping_fee,
    discount,
    customer_id,
    discount - amount AS discount_minus_amount
FROM orders
WHERE customer_id IS NOT NULL OR channel IS NULL
ORDER BY shipping_fee DESC, discount DESC
