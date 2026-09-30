SELECT
    id,
    store_id,
    order_date,
    shipping_fee,
    ROUND(AVG(shipping_fee) OVER (PARTITION BY store_id ORDER BY order_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) shipping_fee_moving_avg_3
FROM orders;
