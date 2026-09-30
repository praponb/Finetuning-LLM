SELECT
    store_id,
    DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month,
    YEAR(sale_date) AS sale_year,
    COUNT(DISTINCT revenue) AS unique_stores
FROM store_sales
GROUP BY store_id, DATE_FORMAT(sale_date, 'yyyy-MM'), YEAR(sale_date)
HAVING COUNT(DISTINCT revenue) >= 1
ORDER BY unique_stores DESC;
