SELECT
    id,
    sale_date,
    product_id,
    units
FROM store_sales
ORDER BY units DESC, product_id;
