WITH flattened_attrs AS (
    SELECT
        id,
        coupon_codes,
        items,
        attributes,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'device') AS has_device,
        SIZE(items) AS num_items,
        ship_to.country AS ship_country,
        attributes['device'] AS device
    FROM carts
)
SELECT
    id,
    coupon_codes,
    has_device,
    num_items,
    ship_country,
    device,
    STDDEV_SAMP(num_items) OVER (PARTITION BY ship_country) AS ship_country_stddev_samp_num_items,
    CUME_DIST() OVER (ORDER BY num_items NULLS LAST) AS num_items_cume_dist
FROM flattened_attrs;
