SELECT
    id,
    sale_date,
    revenue,
    product_id
FROM store_sales
WHERE sale_date = (SELECT MAX(sale_date) FROM store_sales WHERE store_id = 1)
ORDER BY sale_date;
