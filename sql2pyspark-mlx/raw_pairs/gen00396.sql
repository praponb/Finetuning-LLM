SELECT
    DATE_TRUNC('MONTH', sale_date) AS sale_month,
    ROUND(STDDEV(revenue), 2) AS stddev_revenue,
    SUM(revenue) AS total_revenue,
    MAX(sale_date) AS last_sale_date
FROM store_sales
WHERE revenue < 1200
GROUP BY sale_month
