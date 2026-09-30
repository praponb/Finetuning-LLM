SELECT
    i.last_restock,
    i.product_id,
    i.store_id,
    ss.sale_date,
    ss.product_id AS store_sale_product_id,
    FLOOR(ss.units) AS units_whole
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE YEAR(i.last_restock) >= 2025
ORDER BY ss.sale_date, store_id
