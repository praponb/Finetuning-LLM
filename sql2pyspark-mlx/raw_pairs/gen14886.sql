SELECT
    id,
    customer_id,
    status,
    shipping_fee,
    AVG(shipping_fee) OVER (PARTITION BY DATE_TRUNC('WEEK', order_date), channel ORDER BY discount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_2,
    NTILE(4) OVER (PARTITION BY MONTH(order_date), customer_id ORDER BY shipping_fee, id) AS shipping_fee_bucket
FROM orders
WHERE order_date < '2023-04-10';
