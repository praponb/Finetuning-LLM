SELECT
    revenue,
    units,
    sale_date,
    COALESCE(revenue, 0) AS revenue_filled,
    CAST(units AS STRING) AS units_num
FROM store_sales
ORDER BY units;
