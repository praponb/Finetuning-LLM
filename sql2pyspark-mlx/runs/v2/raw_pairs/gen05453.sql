SELECT
    customer_id,
    store_id,
    discount * shipping_fee AS discount_times_shipping_fee,
    CONCAT_WS(',', status, channel) AS status_channel_key
FROM orders
ORDER BY 3 DESC
