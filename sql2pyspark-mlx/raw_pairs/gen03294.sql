SELECT
    store_id,
    product_id,
    COUNT(*) AS row_count,
    ROUND(AVG(units), 2) AS avg_units,
    MIN(sale_date) AS min_sale_date
FROM store_sales
GROUP BY store_id, product_id
HAVING COUNT(*) >= 5
ORDER BY avg_units DESC, 1;
