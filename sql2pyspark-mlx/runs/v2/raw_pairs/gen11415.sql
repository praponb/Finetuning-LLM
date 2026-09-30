SELECT
    id,
    discount,
    order_date,
    channel
FROM orders
WHERE discount > (SELECT MAX(discount) FROM orders WHERE store_id = 33)
