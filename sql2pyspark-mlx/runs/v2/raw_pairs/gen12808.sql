SELECT
    id,
    store_id,
    sale_date,
    units,
    LEAD(units, 1) OVER (PARTITION BY store_id ORDER BY sale_date, id) next_units
FROM store_sales
WHERE product_id = 10
