WITH enriched AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY customer_id ORDER BY discount, id) AS discount_bucket,
        LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY status ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM orders
)
SELECT
    id,
    store_id,
    channel,
    amount,
    discount,
    shipping_fee,
    discount_bucket,
    last_known_channel
FROM enriched
ORDER BY store_id DESC NULLS FIRST;
