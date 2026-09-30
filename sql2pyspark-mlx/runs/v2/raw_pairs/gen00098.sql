SELECT
    product_id,
    DATE_TRUNC('MONTH', sale_date) AS sale_month,
    ROUND(AVG(revenue), 2) AS avg_revenue,
    ROUND(STDDEV(revenue), 2) AS stddev_revenue
FROM store_sales
WHERE units BETWEEN 16 AND 20
GROUP BY product_id, DATE_TRUNC('MONTH', sale_date)
ORDER BY stddev_revenue DESC, avg_revenue DESC, product_id, sale_month
LIMIT 10;
