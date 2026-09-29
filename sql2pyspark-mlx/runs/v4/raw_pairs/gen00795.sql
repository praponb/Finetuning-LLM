SELECT
    id,
    store_id,
    sale_date,
    product_id,
    units,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id) AS sale_date_row,
    LEAD(units, 1) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS next_units
FROM store_sales
