SELECT
    id,
    order_date,
    store_id,
    amount,
    DENSE_RANK() OVER (ORDER BY order_date DESC) AS dense_rnk,
    SUM(amount) OVER (PARTITION BY store_id ORDER BY order_date, id) AS running_amount
FROM orders;
