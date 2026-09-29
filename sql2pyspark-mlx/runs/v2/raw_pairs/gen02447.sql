SELECT
    id,
    store_id,
    product_id,
    sale_date,
    units,
    LEAD(units, 1) OVER (PARTITION BY store_id, product_id ORDER BY sale_date, id) AS next_units
FROM store_sales
WHERE sale_date BETWEEN '2025-07-01' AND '2025-09-01'
ORDER BY units DESC;
