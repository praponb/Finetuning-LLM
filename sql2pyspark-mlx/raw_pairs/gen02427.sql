SELECT
    id,
    sale_date,
    store_id,
    revenue
FROM store_sales
WHERE sale_date = (SELECT MIN(sale_date) FROM store_sales)
ORDER BY 3 ASC;
