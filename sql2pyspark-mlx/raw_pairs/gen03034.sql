SELECT
    id,
    shipping_fee,
    store_id,
    discount - amount AS discount_minus_amount
FROM orders
WHERE order_date >= '2023-11-01' AND shipping_fee IS NULL AND status NOT IN ('COMPLETED', 'PENDING')
ORDER BY store_id DESC;
