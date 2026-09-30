SELECT
    id,
    channel,
    order_date,
    amount,
    discount,
    amount - LAG(amount, 1) OVER (PARTITION BY channel ORDER BY order_date, id) AS amount_change,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount DESC, id) AS rn
FROM orders
WHERE YEAR(order_date) >= 2024;
