SELECT
    discount,
    store_id,
    LENGTH(channel) AS channel_length,
    shipping_fee + discount AS shipping_fee_plus_discount
FROM orders
ORDER BY store_id DESC, channel_length DESC
