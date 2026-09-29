SELECT
    id,
    status,
    order_date,
    shipping_fee,
    LEAD(shipping_fee, 1) OVER (PARTITION BY status ORDER BY order_date, id) AS next_shipping_fee
FROM orders;
