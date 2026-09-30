SELECT
    store_sales.id,
    store_sales.units,
    store_sales.product_id,
    stores.store_name,
    stores.sqft
FROM store_sales
LEFT JOIN stores ON stores.id = store_sales.store_id
ORDER BY units DESC, stores.store_name DESC, id
LIMIT 10;
