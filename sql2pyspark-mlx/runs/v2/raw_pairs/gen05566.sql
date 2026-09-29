SELECT
    stores.store_name,
    store_sales.store_id,
    store_sales.units,
    store_sales.product_id
FROM stores
INNER JOIN store_sales ON stores.id = store_sales.store_id
ORDER BY units;
