SELECT product_id, MIN(revenue) AS min_revenue, ROUND(STDDEV(revenue), 2) AS stddev_revenue
FROM store_sales
GROUP BY product_id
ORDER BY stddev_revenue DESC
