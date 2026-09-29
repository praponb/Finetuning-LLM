SELECT
    id,
    units,
    revenue,
    units + revenue AS revenue_per_unit
FROM store_sales;
