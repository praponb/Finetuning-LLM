SELECT
    id,
    store_id,
    units,
    product_id,
    sale_date,
    ROUND(units / SUM(units) OVER (PARTITION BY store_id), 3) AS units_share,
    LAG(units, 2) OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id) AS prev_units
FROM store_sales
