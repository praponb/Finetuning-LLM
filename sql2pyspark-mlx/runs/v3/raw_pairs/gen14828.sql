SELECT
    units,
    revenue,
    sale_date,
    CAST(units AS STRING) AS units_str,
    ROUND(revenue / units, 2) AS revenue_per_unit
FROM store_sales
ORDER BY sale_date
