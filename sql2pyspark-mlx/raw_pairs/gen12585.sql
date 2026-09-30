WITH base AS (
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        CAST(units AS STRING) AS units_num
    FROM store_sales
    WHERE revenue <= 1000
)
SELECT
    product_id,
    CASE WHEN revenue >= 3800 THEN 'LARGE' ELSE 'SMALL' END AS revenue_tier,
    AVG(units) AS avg_units,
    APPROX_PERCENTILE(revenue, 0.9) AS revenue_p90,
    APPROX_PERCENTILE(units, 0.95) AS units_p95,
    PERCENTILE_APPROX(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(units), 2) AS avg_units_2
FROM base
GROUP BY 1, 2
ORDER BY revenue_tier DESC;
