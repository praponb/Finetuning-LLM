WITH filtered AS (
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue,
        revenue * units AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > DATE '2023-08-10'
      AND product_id IN (6, 7, 8)
)
SELECT
    DATE_TRUNC('MONTH', sale_date) AS activity_month,
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    BIT_OR(units) AS units_bit_or,
    PERCENTILE_APPROX(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
    STDDEV_SAMP(revenue) AS revenue_stddev,
    COUNT(DISTINCT id) AS distinct_ids
FROM filtered
GROUP BY ROLLUP (DATE_TRUNC('MONTH', sale_date), DATE_TRUNC('QUARTER', sale_date))
ORDER BY units_bit_or;
