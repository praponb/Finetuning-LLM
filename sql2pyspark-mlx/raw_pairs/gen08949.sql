SELECT
    id,
    units,
    revenue,
    revenue - units AS revenue_per_unit,
    FLOOR(units) AS units_whole
FROM store_sales
