SELECT
    id,
    customer_id,
    channel,
    shipping_fee,
    ROW_NUMBER() OVER (PARTITION BY customer_id, channel ORDER BY shipping_fee DESC, id) AS row_num
FROM orders
WHERE discount <= 0.16
ORDER BY channel DESC, id DESC;
