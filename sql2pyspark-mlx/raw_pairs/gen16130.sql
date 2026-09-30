SELECT
    id,
    customer_id,
    ship_to,
    attributes,
    COALESCE(attributes['utm_source'], 'unknown') AS utm_source_or_default,
    TRANSFORM(items, li -> li.product_id) AS product_ids,
    ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text,
    attributes['device'] AS device
FROM carts;
