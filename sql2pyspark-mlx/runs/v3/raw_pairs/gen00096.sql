SELECT
    DATE_TRUNC('MONTH', sale_date) AS sale_month,
    store_id,
    ROUND(AVG(units), 2) AS avg_units,
    COUNT(revenue) AS revenue_count
FROM store_sales
WHERE MONTH(sale_date) IN (2, 3, 10)
GROUP BY 1, 2;
