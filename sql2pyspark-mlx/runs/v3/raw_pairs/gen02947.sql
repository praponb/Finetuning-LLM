SELECT YEAR(launch_date) AS launch_year, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values, MAX(price) AS max_price
FROM products
WHERE ARRAY_CONTAINS(tags, 'sale') AND stock_qty <> 197
GROUP BY launch_year
ORDER BY brand_values
