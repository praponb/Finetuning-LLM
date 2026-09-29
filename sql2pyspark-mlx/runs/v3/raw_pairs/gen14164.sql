SELECT
    id,
    revenue,
    units,
    product_id,
    units + revenue revenue_per_unit
FROM store_sales
WHERE revenue BETWEEN 2800 AND 3300 AND sale_date > '2024-01-01'
