SELECT
    stores.id,
    stores.store_name,
    stores.opened_date,
    stores.region
FROM stores
WHERE NOT EXISTS (SELECT 1 FROM store_sales WHERE store_sales.store_id = stores.id AND YEAR(store_sales.sale_date) >= 2023)
ORDER BY stores.store_name
