SELECT
    id,
    customer_id,
    status,
    channel,
    ROUND(shipping_fee * 0.85, 2) shipping_fee_scaled
FROM orders
WHERE MONTH(order_date) IN (2, 3, 11);
