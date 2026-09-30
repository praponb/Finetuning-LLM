SELECT *
FROM (
    SELECT
        id,
        store_id,
        order_date,
        customer_id,
        RANK() OVER (PARTITION BY store_id ORDER BY order_date DESC) pos
    FROM orders
) t
WHERE pos <= 3
ORDER BY store_id DESC, id
