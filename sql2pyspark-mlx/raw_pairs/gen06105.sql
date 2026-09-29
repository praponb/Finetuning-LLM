SELECT
    id,
    sale_date,
    product_id,
    units,
    SUM(units) OVER (PARTITION BY YEAR(sale_date), product_id) AS sale_date_year_sum_units
FROM store_sales
WHERE revenue >= 1700;
