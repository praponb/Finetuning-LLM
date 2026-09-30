SELECT
    id,
    channel,
    shipping_fee,
    store_id,
    discount,
    PERCENT_RANK() OVER (PARTITION BY channel ORDER BY shipping_fee DESC) AS shipping_fee_pct_rank,
    MIN(discount) OVER (PARTITION BY store_id) AS store_id_min_discount
FROM orders
ORDER BY channel
