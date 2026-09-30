SELECT
    id,
    units,
    sale_date,
    product_id
FROM store_sales
WHERE units >= (SELECT MAX(units) FROM store_sales)
ORDER BY id DESC, units DESC;
