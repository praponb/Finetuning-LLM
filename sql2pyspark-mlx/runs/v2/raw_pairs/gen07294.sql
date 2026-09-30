SELECT
    product_id,
    revenue,
    store_id,
    FLOOR(units) AS units_whole
FROM store_sales
WHERE store_id = 2
ORDER BY product_id
