SELECT
    id,
    store_id,
    order_date,
    DENSE_RANK() OVER (PARTITION BY store_id ORDER BY order_date) dense_rnk
FROM orders
ORDER BY id DESC;
