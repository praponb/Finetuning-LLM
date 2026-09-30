SELECT
    id,
    discount,
    channel,
    order_date,
    amount,
    PERCENT_RANK() OVER (ORDER BY discount DESC) AS discount_pct_rank,
    SUM(amount) OVER (PARTITION BY channel ORDER BY order_date, id) AS running_amount
FROM orders
ORDER BY channel
