SELECT
    id,
    order_date,
    shipping_fee,
    store_id
FROM orders
ORDER BY store_id DESC, id
LIMIT 50;
