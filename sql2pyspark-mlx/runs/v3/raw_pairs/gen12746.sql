SELECT
    id,
    customer_id,
    order_date,
    shipping_fee,
    ROUND(AVG(shipping_fee) OVER (PARTITION BY customer_id ORDER BY order_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS shipping_fee_moving_avg_4
FROM orders
WHERE status IN ('COMPLETED', 'PENDING');
