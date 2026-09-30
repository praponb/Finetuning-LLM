SELECT
    id,
    product_id,
    store_id,
    COUNT(*) OVER (PARTITION BY product_id, store_id) product_id_count
FROM store_sales
