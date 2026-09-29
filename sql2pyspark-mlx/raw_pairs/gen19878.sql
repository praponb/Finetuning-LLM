SELECT
    DATE_TRUNC('YEAR', sale_date) AS year_start,
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    APPROX_PERCENTILE(units, 0.5) AS units_p50,
    ROUND(AVG(revenue), 2) AS avg_revenue,
    ROUND(AVG(units), 2) AS avg_units,
    MIN(units) AS min_units,
    APPROX_PERCENTILE(units, 0.9) AS units_p90,
    CORR(units, revenue) AS corr_units_revenue,
    GROUPING(DATE_TRUNC('YEAR', sale_date)) AS gid
FROM store_sales
GROUP BY CUBE (DATE_TRUNC('YEAR', sale_date), DATE_TRUNC('WEEK', sale_date))
HAVING min_units > 0
ORDER BY avg_units DESC NULLS FIRST
