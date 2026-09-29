SELECT
    channel,
    shipping_fee,
    status,
    ROUND(amount * 0.9, 2) AS amount_adjusted
FROM orders
WHERE order_date BETWEEN '2024-02-01' AND '2025-07-15' AND shipping_fee IS NOT NULL
ORDER BY status DESC;
