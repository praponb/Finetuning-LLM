WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        items,
        SIZE(attributes) AS num_attributes,
        COALESCE(ship_to.zip, 'UNKNOWN') AS zip_or_unknown,
        ARRAY_CONTAINS(coupon_codes, 'BUNDLE') AS used_bundle,
        ship_to.country AS ship_country,
        ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text
    FROM carts
)
-- Grouped metrics
SELECT
    CASE WHEN num_attributes >= 72 THEN 'HIGH' ELSE 'LOW' END AS num_attributes_tier,
    ship_country,
    SIZE(COLLECT_SET(ship_country)) AS num_ship_countries,
    ROUND(AVG(num_attributes), 2) AS avg_num_attributes,
    PERCENTILE_APPROX(num_attributes, 0.95) AS num_attributes_p95,
    COUNT_IF(ship_country = 'UK') AS uk_events,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN num_attributes >= 72 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT_IF(ship_country = 'UK') DESC) AS uk_events_percent_rank
FROM parsed
GROUP BY CASE WHEN num_attributes >= 72 THEN 'HIGH' ELSE 'LOW' END, ship_country;
