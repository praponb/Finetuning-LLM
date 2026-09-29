SELECT
    id,
    customer_id,
    amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY customer_id), 3) amount_share,
    ROW_NUMBER() OVER (ORDER BY amount, id) row_num
FROM orders;
