SELECT sale_month, total_revenue, max_units
FROM (
    SELECT DATE_TRUNC('MONTH', sale_date) AS sale_month, SUM(revenue) AS total_revenue, MAX(units) AS max_units
    FROM store_sales
    GROUP BY DATE_TRUNC('MONTH', sale_date)
) AS t
WHERE max_units >= 30
ORDER BY 1 DESC;
