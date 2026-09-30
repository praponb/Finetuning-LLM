SELECT
    product_id,
    QUARTER(sale_date) AS sale_quarter,
    ROUND(STDDEV(revenue), 2) AS stddev_revenue,
    AVG(units) AS avg_units
FROM store_sales
GROUP BY product_id, QUARTER(sale_date)
HAVING COUNT(*) > 1
