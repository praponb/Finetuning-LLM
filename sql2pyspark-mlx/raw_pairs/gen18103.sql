WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        customer_id,
        coupon_codes,
        ship_to,
        COALESCE(attributes['device'], 'unknown') AS device_or_default,
        SIZE(items) AS num_items,
        attributes['utm_source'] AS utm_source,
        ship_to.country AS ship_country
    FROM carts
)
-- Running and moving aggregates
SELECT
    id,
    customer_id,
    coupon_codes,
    device_or_default,
    num_items,
    utm_source,
    AVG(num_items) OVER (ORDER BY num_items, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) num_items_avg_2,
    ROW_NUMBER() OVER (ORDER BY num_items DESC, id) seq_no,
    MIN(num_items) OVER (PARTITION BY ship_country, customer_id) ship_country_min_num_items
FROM flattened_attrs
WHERE ship_country IN ('SG', 'TH', 'DE')
