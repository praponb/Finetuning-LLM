SELECT
    s.region,
    o.store_id,
    o.shipping_fee,
    CONCAT_WS(',', o.status, o.channel) AS status_channel_key
FROM stores s
LEFT OUTER JOIN orders o ON o.store_id = s.id
WHERE o.discount >= 0.3
