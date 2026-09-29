SELECT
    QUARTER(sale_date) AS sale_quarter,
    YEAR(sale_date) AS sale_year,
    store_id,
    MAX(revenue) AS max_revenue
FROM store_sales
GROUP BY 1, 2, 3;
