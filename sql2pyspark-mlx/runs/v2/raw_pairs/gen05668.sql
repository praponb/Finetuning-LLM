SELECT
    id,
    units,
    revenue,
    revenue - units AS revenue_per_unit
FROM store_sales
WHERE revenue >= 3800
