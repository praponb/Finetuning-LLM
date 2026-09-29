SELECT
    id,
    channel,
    status,
    shipping_fee,
    PERCENT_RANK() OVER (PARTITION BY channel, status ORDER BY shipping_fee DESC) AS shipping_fee_pct_rank
FROM orders
WHERE store_id IS NOT NULL
ORDER BY channel;
