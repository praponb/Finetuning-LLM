SELECT
    i.id,
    i.store_id,
    i.product_id,
    i.last_restock,
    ss.store_id store_sale_store_id,
    ROUND(ss.revenue / ss.units, 2) revenue_per_unit
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY store_id DESC
