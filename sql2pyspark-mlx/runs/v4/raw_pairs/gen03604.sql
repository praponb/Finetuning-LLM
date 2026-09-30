SELECT
    id,
    order_date,
    discount,
    ROUND(AVG(discount) OVER (ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS discount_moving_avg_7
FROM orders
WHERE channel = 'store';
