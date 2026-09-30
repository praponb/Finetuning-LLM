SELECT
    id,
    product_id,
    sale_date,
    units,
    LEAD(units, 2) OVER (PARTITION BY product_id ORDER BY sale_date, id) next_units,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) rn
FROM store_sales
