WITH deduped AS (
    SELECT *
    FROM store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue, id) = 1
)
SELECT
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    product_id,
    ROUND(AVG(units), 2) AS avg_units,
    PERCENTILE_APPROX(units, 0.5) AS units_p50,
    APPROX_PERCENTILE(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
    COUNT(DISTINCT product_id) AS distinct_product_ids
FROM deduped
GROUP BY quarter_start, product_id
ORDER BY units_p50 DESC
