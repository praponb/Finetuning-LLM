SELECT
    DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month,
    ROUND(STDDEV(units), 2) AS stddev_units,
    COUNT(DISTINCT revenue) AS unique_stores,
    MAX(units) - MIN(units) AS units_range
FROM store_sales
GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM')
ORDER BY sale_month;
