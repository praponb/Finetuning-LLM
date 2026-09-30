SELECT
    id,
    order_date,
    store_id,
    shipping_fee,
    SUM(shipping_fee) OVER (PARTITION BY YEAR(order_date), store_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_shipping_fee
FROM orders
WHERE channel = 'web';
