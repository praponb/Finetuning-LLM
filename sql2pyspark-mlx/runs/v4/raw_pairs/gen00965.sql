SELECT
    id,
    store_id,
    sale_date,
    units,
    LAST_VALUE(units) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_units
FROM store_sales;
