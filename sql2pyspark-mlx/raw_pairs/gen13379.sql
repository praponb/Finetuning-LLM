SELECT
    id,
    store_id,
    revenue,
    GREATEST(units, revenue) AS greatest_units_revenue
FROM store_sales
WHERE sale_date < '2024-04-01'
