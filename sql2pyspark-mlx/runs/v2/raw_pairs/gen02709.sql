SELECT
    id,
    sale_date,
    store_id,
    product_id
FROM store_sales
WHERE sale_date = (SELECT MIN(sale_date) FROM store_sales WHERE store_id IN (1, 6));
