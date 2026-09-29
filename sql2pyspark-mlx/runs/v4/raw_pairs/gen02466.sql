WITH unpacked AS (
    SELECT
        id,
        customer_id,
        items,
        SIZE(FILTER(items, i -> i.qty >= 3)) AS bulk_lines,
        ship_to.zip AS ship_zip
    FROM carts
    WHERE customer_id IN (7, 15, 21, 39)
)
SELECT
    CASE WHEN bulk_lines >= 3 THEN 'HIGH' ELSE 'LOW' END AS bulk_lines_tier,
    customer_id,
    APPROX_PERCENTILE(bulk_lines, 0.5) AS bulk_lines_p50,
    APPROX_PERCENTILE(bulk_lines, 0.5) AS bulk_lines_p50_2
FROM unpacked
GROUP BY GROUPING SETS ((CASE WHEN bulk_lines >= 3 THEN 'HIGH' ELSE 'LOW' END, customer_id), (CASE WHEN bulk_lines >= 3 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING APPROX_PERCENTILE(bulk_lines, 0.5) >= 0
