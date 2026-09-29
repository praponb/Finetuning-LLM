SELECT
    id,
    product_id,
    sale_date,
    units,
    LEAD(units, 2) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS next_units
FROM store_sales
WHERE sale_date > '2024-07-01'
ORDER BY sale_date, id DESC;
