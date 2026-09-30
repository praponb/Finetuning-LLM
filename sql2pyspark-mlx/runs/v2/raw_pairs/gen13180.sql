SELECT
    id,
    units,
    product_id,
    revenue,
    units * revenue AS revenue_per_unit
FROM store_sales
WHERE revenue <= 3500
