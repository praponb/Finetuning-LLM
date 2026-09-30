SELECT
    inventory.product_id,
    store_sales.product_id store_sale_product_id,
    store_sales.units,
    store_sales.sale_date
FROM inventory
JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
