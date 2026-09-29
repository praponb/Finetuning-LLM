SELECT
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    SUM(revenue) AS total_revenue,
    BIT_AND(units) AS units_bit_and,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    PERCENTILE_APPROX(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles
FROM store_sales
GROUP BY 1
HAVING units_bit_and > 1
