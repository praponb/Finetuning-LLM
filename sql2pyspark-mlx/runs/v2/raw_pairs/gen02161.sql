SELECT
    id,
    revenue,
    store_id,
    units,
    LEAST(revenue, units) AS least_revenue_units,
    units * revenue AS revenue_per_unit
FROM store_sales
