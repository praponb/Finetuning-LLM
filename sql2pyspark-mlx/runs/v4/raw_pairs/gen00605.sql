SELECT
    store_id,
    product_id,
    YEAR(sale_date) AS sale_year,
    MIN(revenue) AS min_revenue
FROM store_sales
GROUP BY store_id, product_id, sale_year
ORDER BY product_id;
