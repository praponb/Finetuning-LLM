SELECT
    id,
    store_id,
    units,
    product_id,
    sale_date,
    revenue,
    ROUND(units / SUM(units) OVER (PARTITION BY store_id), 3) AS units_share,
    LAG(revenue, 1, 0) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS prev_revenue
FROM store_sales
