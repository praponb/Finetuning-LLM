SELECT
    id,
    customer_id,
    status,
    CEIL(discount) AS discount_whole
FROM orders
