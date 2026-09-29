SELECT
    id,
    product_id,
    sale_date,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) AS sale_date_row
FROM store_sales;
