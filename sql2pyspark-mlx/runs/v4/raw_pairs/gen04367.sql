SELECT
    store_sales.store_id,
    products.cost,
    stores.region,
    stores.sqft
FROM store_sales
JOIN products ON store_sales.product_id = products.id
LEFT JOIN stores ON store_sales.store_id = stores.id
WHERE products.category = 'Grocery'
ORDER BY store_id;
