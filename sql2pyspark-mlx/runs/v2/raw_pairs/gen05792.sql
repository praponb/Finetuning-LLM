SELECT
    id,
    customer_id,
    order_date,
    status,
    CAST(discount AS INT) AS discount_int
FROM orders
WHERE store_id = 4 AND status IN ('PENDING', 'COMPLETED') AND YEAR(order_date) = 2024
ORDER BY customer_id DESC, discount_int DESC, id ASC
LIMIT 10;
