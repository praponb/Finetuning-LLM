SELECT
    store_id,
    discount,
    customer_id,
    ROUND(shipping_fee, 1) AS shipping_fee_rounded
FROM orders
WHERE amount > 1280
ORDER BY shipping_fee_rounded DESC;
