WITH parsed AS (
    SELECT
        id,
        customer_id,
        created_at,
        SIZE(FILTER(items, x -> x.qty >= 3)) AS bulk_lines,
        attributes['gift'] AS gift,
        ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text,
        attributes['utm_source'] AS utm_source,
        attributes['device'] AS device
    FROM carts
)
SELECT
    CASE WHEN bulk_lines >= 3 THEN 'HIGH' ELSE 'LOW' END AS bulk_lines_tier,
    customer_id,
    PERCENTILE_APPROX(bulk_lines, ARRAY(0.25, 0.5, 0.75)) AS bulk_lines_quartiles,
    COUNT(*) AS row_count,
    ROUND(AVG(bulk_lines), 2) AS avg_bulk_lines,
    GROUPING(CASE WHEN bulk_lines >= 3 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM parsed
GROUP BY GROUPING SETS ((CASE WHEN bulk_lines >= 3 THEN 'HIGH' ELSE 'LOW' END, customer_id), (CASE WHEN bulk_lines >= 3 THEN 'HIGH' ELSE 'LOW' END), ())
ORDER BY avg_bulk_lines DESC
