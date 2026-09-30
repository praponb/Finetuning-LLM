WITH unpacked AS (
    SELECT
        id,
        items,
        ship_to,
        attributes,
        SIZE(FILTER(items, i -> i.qty >= 3)) AS bulk_lines,
        SIZE(attributes) AS num_attributes
    FROM carts
)
SELECT
    CASE WHEN num_attributes >= 84 THEN 'LARGE' ELSE 'SMALL' END AS num_attributes_tier,
    CASE WHEN bulk_lines >= 3 THEN 'PREMIUM' ELSE 'STANDARD' END AS bulk_lines_tier,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(bulk_lines, ARRAY(0.25, 0.5, 0.75)) AS bulk_lines_quartiles,
    ROUND(AVG(bulk_lines), 2) AS avg_bulk_lines,
    APPROX_PERCENTILE(num_attributes, ARRAY(0.25, 0.5, 0.75)) AS num_attributes_quartiles,
    PERCENTILE_APPROX(num_attributes, 0.5) AS num_attributes_p50,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN num_attributes >= 84 THEN 'LARGE' ELSE 'SMALL' END ORDER BY ROUND(AVG(bulk_lines), 2) DESC) AS avg_bulk_lines_percent_rank
FROM unpacked
GROUP BY CASE WHEN num_attributes >= 84 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN bulk_lines >= 3 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING ROUND(AVG(bulk_lines), 2) > 0;
