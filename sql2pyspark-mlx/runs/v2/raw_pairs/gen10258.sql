SELECT
    id,
    sale_date,
    product_id,
    store_id
FROM store_sales
WHERE sale_date >= (SELECT MAX(sale_date) FROM store_sales)
ORDER BY store_id DESC, sale_date DESC;
