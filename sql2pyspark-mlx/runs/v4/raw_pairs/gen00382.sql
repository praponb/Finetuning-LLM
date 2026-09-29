SELECT
    id,
    customer_id,
    items,
    ship_to,
    attributes,
    ARRAY_MAX(TRANSFORM(items, li -> li.price)) AS max_item_price,
    SIZE(FILTER(items, li -> li.qty >= 3)) AS bulk_lines
FROM carts;
