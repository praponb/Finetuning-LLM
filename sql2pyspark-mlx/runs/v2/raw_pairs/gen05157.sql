SELECT
    id,
    customer_id,
    order_date,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_date_dense_rank
FROM orders
WHERE store_id IS NOT NULL;
