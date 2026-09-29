SELECT
    id,
    order_id,
    amount,
    NTILE(10) OVER (PARTITION BY order_id ORDER BY amount, id) quartile
FROM payments
WHERE method IN ('card', 'promptpay')
ORDER BY order_id DESC;
