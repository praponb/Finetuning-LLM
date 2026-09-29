SELECT
    id,
    units,
    revenue,
    sale_date
FROM store_sales
WHERE units < (SELECT MIN(units) FROM store_sales);
