SELECT
    id,
    store_id,
    product_id,
    revenue,
    units + revenue AS revenue_per_unit
FROM store_sales
WHERE store_id IN (4, 5, 6) AND units = 16
ORDER BY product_id DESC, id
LIMIT 5;
