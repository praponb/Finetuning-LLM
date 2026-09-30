SELECT
    id,
    customer_id,
    store_id,
    order_date,
    status,
    channel,
    amount,
    discount,
    shipping_fee,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY discount NULLS LAST) AS discount_dense_rank,
    FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
    SUM(shipping_fee) OVER (PARTITION BY status ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee
FROM orders
