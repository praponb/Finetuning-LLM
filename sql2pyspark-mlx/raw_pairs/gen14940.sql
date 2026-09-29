SELECT
    DATE_TRUNC('MONTH', sale_date) sale_month,
    MAX(sale_date) latest_sale_date,
    COUNT(revenue) revenue_count,
    AVG(units) avg_units
FROM store_sales
WHERE store_id IN (1, 4)
GROUP BY 1
HAVING MIN(units) > 20
