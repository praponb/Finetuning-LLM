SELECT
    channel,
    status,
    shipping_fee,
    ROUND(discount / amount, 2) AS discount_per_amount
FROM orders
WHERE amount BETWEEN 510 AND 1370
