SELECT
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    CORR(units, revenue) AS corr_units_revenue,
    BIT_OR(units) AS units_bit_or,
    MAX(revenue) AS max_revenue
FROM store_sales
GROUP BY DATE_TRUNC('WEEK', sale_date)
ORDER BY week_start
