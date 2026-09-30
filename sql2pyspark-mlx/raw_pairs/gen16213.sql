WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        product_id,
        units,
        revenue
    FROM store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id ASC) = 1
)
-- Grouped metrics
SELECT
    CASE WHEN revenue >= 3300 THEN 'HIGH' ELSE 'LOW' END AS revenue_tier,
    product_id,
    PERCENTILE_APPROX(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
    BIT_AND(units) AS units_bit_and
FROM ranked
GROUP BY 1, 2
ORDER BY product_id DESC
