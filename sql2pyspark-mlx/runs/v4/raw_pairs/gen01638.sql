WITH flattened_attrs AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        items,
        ship_to,
        attributes,
        attributes['utm_source'] AS utm_source,
        attributes['device'] AS device,
        COALESCE(attributes['gift'], 'direct') AS gift_or_default,
        FORALL(items, item -> item.qty <= 5) AS all_small_qty,
        EXISTS(items, item -> item.price > 100) AS has_premium_item
    FROM carts
)
SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    items,
    ship_to,
    attributes,
    utm_source,
    device,
    gift_or_default,
    all_small_qty,
    has_premium_item,
    LAST_VALUE(utm_source) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_utm_source,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC, id) AS seq_no,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC, id) AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at, id) AS seq_no_2
FROM flattened_attrs
