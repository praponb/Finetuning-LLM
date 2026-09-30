SELECT
    id,
    store_id,
    order_date,
    discount,
    shipping_fee,
    discount - LAG(discount, 1, 0) OVER (PARTITION BY store_id ORDER BY order_date, id) AS discount_change,
    LEAD(shipping_fee, 1) OVER (PARTITION BY store_id ORDER BY order_date, id) AS next_shipping_fee
FROM orders
ORDER BY next_shipping_fee DESC
