SELECT
    product_id,
    revenue,
    sale_date,
    ADD_MONTHS(sale_date, 3) AS sale_date_plus_months
FROM store_sales
WHERE revenue IS NOT NULL
ORDER BY sale_date ASC, sale_date_plus_months DESC
