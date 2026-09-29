WITH with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.order_discount,
        latest.order_order_date
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.discount AS order_discount, c.order_date AS order_order_date
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Grouped metrics
SELECT
    CASE WHEN order_discount >= 0.37 THEN 'PREMIUM' ELSE 'STANDARD' END AS order_discount_tier,
    country,
    DATE_TRUNC('WEEK', signup_date) AS week_start,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    APPROX_PERCENTILE(age, 0.95) AS age_p95,
    COUNT_IF(segment = 'Consumer') AS consumer_events,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list
FROM with_last
GROUP BY GROUPING SETS ((CASE WHEN order_discount >= 0.37 THEN 'PREMIUM' ELSE 'STANDARD' END, country, DATE_TRUNC('WEEK', signup_date)), (CASE WHEN order_discount >= 0.37 THEN 'PREMIUM' ELSE 'STANDARD' END, country))
ORDER BY week_start;
