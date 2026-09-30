SELECT
    id,
    sale_date,
    product_id,
    units
FROM store_sales
WHERE units BETWEEN 11 AND 15 AND units <= 33
ORDER BY sale_date;
