SELECT
    status,
    MONTH(order_date) AS order_month,
    COUNT(shipping_fee) AS shipping_fee_count,
    MIN(shipping_fee) AS min_shipping_fee
FROM orders
WHERE store_id IS NULL
GROUP BY 1, 2;
