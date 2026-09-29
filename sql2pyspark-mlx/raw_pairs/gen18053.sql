SELECT ROUND(STDDEV(units), 2) AS stddev_units, MAX(units) AS max_units, ROUND(STDDEV(revenue), 2) AS stddev_revenue
FROM store_sales
WHERE (product_id = 6 AND store_id = 5) OR revenue >= 3500;
