WITH flattened_attrs AS (
    SELECT
        id,
        created_at,
        items,
        ship_to,
        attributes['utm_source'] AS utm_source,
        ARRAY_MAX(TRANSFORM(items, item -> item.price)) AS max_item_price,
        attributes['gift'] AS gift,
        SIZE(items) AS num_items,
        EXISTS(items, item -> item.price > 100) AS has_premium_item
    FROM carts
    WHERE created_at < '2023-11-01'
)
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    APPROX_PERCENTILE(max_item_price, ARRAY(0.25, 0.5, 0.75)) AS max_item_price_quartiles,
    MIN(max_item_price) AS min_max_item_price
FROM flattened_attrs
GROUP BY DATE_TRUNC('DAY', created_at)
ORDER BY activity_day;
