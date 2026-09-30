SELECT
    id,
    customer_id,
    created_at,
    attributes,
    TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
    SIZE(items) AS num_items,
    ship_to.zip AS ship_zip,
    TRANSFORM(items, li -> li.product_id) AS product_ids,
    ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text
FROM carts
