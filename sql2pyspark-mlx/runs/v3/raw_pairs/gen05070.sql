SELECT
    id,
    sale_date,
    product_id,
    store_id
FROM store_sales
WHERE sale_date = (SELECT MIN(sale_date) FROM store_sales)
