SELECT
    id,
    product_id,
    sale_date,
    units,
    store_id,
    SUM(units) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units,
    ROUND(units / SUM(units) OVER (PARTITION BY store_id), 3) AS units_share
FROM store_sales
WHERE sale_date < '2024-01-30'
