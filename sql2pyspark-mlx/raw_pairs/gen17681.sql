SELECT
    id,
    customer_id,
    channel,
    order_date,
    RANK() OVER (PARTITION BY customer_id, channel ORDER BY order_date) AS rnk
FROM orders
WHERE store_id IS NULL
ORDER BY 2 DESC;
