SELECT
    YEAR(sale_date) sale_year,
    store_id,
    COUNT(*) total_count,
    MAX(units) - MIN(units) units_range,
    SUM(revenue) total_revenue
FROM store_sales
WHERE NOT ((revenue <= 2000 AND units >= 19) OR product_id IN (5, 6, 9))
GROUP BY YEAR(sale_date), store_id;
