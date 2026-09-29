SELECT
    id,
    store_id,
    amount,
    AVG(amount) OVER (PARTITION BY store_id) AS store_id_avg_amount
FROM orders
