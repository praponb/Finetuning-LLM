SELECT
    id,
    sale_date,
    store_id,
    product_id,
    WEEKOFYEAR(sale_date) AS sale_date_weekofyear
FROM store_sales
ORDER BY sale_date;
