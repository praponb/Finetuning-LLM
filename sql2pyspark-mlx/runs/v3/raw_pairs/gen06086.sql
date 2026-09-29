SELECT
    id,
    sale_date,
    store_id,
    product_id,
    units,
    ROW_NUMBER() OVER (ORDER BY sale_date, id) AS sale_date_row,
    ROUND(units / SUM(units) OVER (PARTITION BY store_id, product_id), 3) AS units_share
FROM store_sales
WHERE units = 29
