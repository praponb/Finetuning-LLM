SELECT
    DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month,
    SUM(units) AS total_units,
    COUNT(*) AS cnt,
    SUM(SUM(units)) OVER (ORDER BY DATE_FORMAT(sale_date, 'yyyy-MM')) AS cumulative_units
FROM store_sales
GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM')
ORDER BY cumulative_units DESC, cnt DESC;
