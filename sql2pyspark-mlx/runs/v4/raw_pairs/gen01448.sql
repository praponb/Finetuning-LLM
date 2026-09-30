SELECT
    DATE_FORMAT(products.launch_date, 'yyyy-MM') AS launch_month,
    products.brand,
    ROUND(AVG(store_sales.units), 2) AS avg_units,
    AVG(store_sales.revenue) AS avg_revenue
FROM store_sales
INNER JOIN products ON store_sales.product_id = products.id
GROUP BY DATE_FORMAT(products.launch_date, 'yyyy-MM'), products.brand
ORDER BY brand
