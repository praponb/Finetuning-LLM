SELECT
    id,
    status,
    shipping_fee,
    store_id,
    CONCAT_WS('-', channel, status) AS channel_status_key
FROM orders
