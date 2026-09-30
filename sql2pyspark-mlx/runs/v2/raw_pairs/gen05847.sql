SELECT
    stores.id,
    stores.city,
    COUNT(store_sales.id) AS store_sale_count,
    MIN(store_sales.units) AS min_units
FROM stores
LEFT JOIN store_sales ON store_sales.store_id = stores.id
WHERE stores.sqft > 7310
GROUP BY stores.id, stores.city
HAVING COUNT(store_sales.id) > 1
