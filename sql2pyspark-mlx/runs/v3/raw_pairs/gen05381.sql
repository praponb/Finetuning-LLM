SELECT
    id,
    units,
    product_id,
    DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_date_label
FROM store_sales
WHERE product_id IN (2, 3, 4, 7) AND units > 21;
