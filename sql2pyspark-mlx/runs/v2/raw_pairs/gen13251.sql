SELECT
    id,
    channel,
    order_date,
    amount,
    SUM(amount) OVER (PARTITION BY channel ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_amount
FROM orders;
