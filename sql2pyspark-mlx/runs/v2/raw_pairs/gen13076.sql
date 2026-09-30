SELECT product_id, ROUND(STDDEV(revenue), 2) AS stddev_revenue
FROM store_sales
WHERE store_id IN (1, 2, 4, 6)
GROUP BY product_id
ORDER BY stddev_revenue ASC;
