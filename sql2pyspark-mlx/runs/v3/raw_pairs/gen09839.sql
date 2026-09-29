SELECT
    shipping_fee,
    status,
    customer_id,
    ROUND(discount * 0.85, 2) AS discount_adjusted
FROM orders
WHERE customer_id IS NOT NULL AND (store_id = 7 OR amount > 1450)
