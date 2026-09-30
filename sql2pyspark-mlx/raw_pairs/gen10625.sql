SELECT
    id,
    sale_date,
    units,
    product_id,
    LEAD(units, 1) OVER (ORDER BY sale_date, id) AS next_units,
    RANK() OVER (PARTITION BY product_id ORDER BY sale_date) AS sale_date_rank
FROM store_sales
