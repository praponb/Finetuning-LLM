WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        created_at,
        coupon_codes,
        items,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
        SIZE(attributes) AS num_attributes
    FROM carts
    WHERE SIZE(coupon_codes) > 2
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    PERCENTILE_APPROX(num_attributes, ARRAY(0.25, 0.5, 0.75)) AS num_attributes_quartiles,
    ROUND(AVG(num_attributes), 2) AS avg_num_attributes,
    APPROX_PERCENTILE(num_attributes, 0.95) AS num_attributes_p95,
    SUM(num_attributes) AS total_num_attributes
FROM unpacked
GROUP BY DATE_TRUNC('HOUR', created_at);
