SELECT
    id,
    customer_id,
    channel,
    shipping_fee,
    LAST_VALUE(discount) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('YEAR', order_date), customer_id ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount,
    LAST_VALUE(channel, TRUE) OVER w_ordered AS last_known_channel
FROM orders
WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
