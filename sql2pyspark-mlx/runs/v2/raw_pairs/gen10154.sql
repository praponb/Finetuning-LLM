SELECT
    store_sales.product_id,
    store_sales.store_id,
    stores.store_name,
    stores.sqft,
    stores.opened_date,
    CONCAT(stores.region, ' - ', stores.city) AS region_city
FROM store_sales
JOIN stores ON store_sales.store_id = stores.id
WHERE stores.region = 'South' AND store_sales.revenue < 1500
ORDER BY 6 DESC, stores.sqft;
