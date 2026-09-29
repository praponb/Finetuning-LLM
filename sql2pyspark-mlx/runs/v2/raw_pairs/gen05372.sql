SELECT
    product_id,
    sale_month,
    revenue_range,
    avg_units
FROM (
    SELECT
        product_id,
        DATE_TRUNC('MONTH', sale_date) AS sale_month,
        MAX(revenue) - MIN(revenue) AS revenue_range,
        AVG(units) AS avg_units
    FROM store_sales
    GROUP BY product_id, DATE_TRUNC('MONTH', sale_date)
) AS agg
WHERE revenue_range > 53
ORDER BY revenue_range, product_id, sale_month
LIMIT 5
