WITH flattened_attrs AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        items,
        ship_to,
        attributes,
        ARRAY_MAX(TRANSFORM(items, li -> li.price)) AS max_item_price,
        COALESCE(ship_to.zip, 'UNKNOWN') AS zip_or_unknown,
        attributes['utm_source'] AS utm_source
    FROM carts
)
SELECT
    id,
    customer_id,
    max_item_price,
    LAG(max_item_price, 1) OVER (ORDER BY created_at, id) AS prev_max_item_price,
    LEAD(max_item_price, 1) OVER (PARTITION BY customer_id ORDER BY max_item_price, id) AS next_max_item_price
FROM flattened_attrs
