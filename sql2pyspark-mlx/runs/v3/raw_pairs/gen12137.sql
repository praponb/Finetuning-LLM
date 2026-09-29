SELECT
    YEAR(sale_date) AS sale_year,
    product_id,
    MIN(sale_date) AS min_sale_date,
    AVG(revenue) AS avg_revenue,
    MIN(units) AS min_units
FROM store_sales
GROUP BY ROLLUP(YEAR(sale_date), product_id)
