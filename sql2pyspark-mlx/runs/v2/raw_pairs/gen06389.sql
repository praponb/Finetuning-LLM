SELECT
    id,
    customer_id,
    status,
    shipping_fee,
    channel,
    discount,
    RANK() OVER (PARTITION BY customer_id, status ORDER BY shipping_fee DESC) rnk,
    SUM(discount) OVER (PARTITION BY channel) channel_sum_discount
FROM orders;
