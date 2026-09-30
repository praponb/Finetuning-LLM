SELECT store_id, product_id, ROUND(STDDEV(revenue), 2) AS stddev_revenue
FROM store_sales
WHERE revenue <= 2600 AND MONTH(sale_date) IN (1, 3, 7)
GROUP BY store_id, product_id
ORDER BY 1;
