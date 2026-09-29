SELECT CASE WHEN YEAR(products.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(products.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort, MAX(order_items.unit_price) - MIN(order_items.unit_price) AS unit_price_range
FROM order_items
INNER JOIN products ON order_items.product_id = products.id
WHERE products.stock_qty >= 195
GROUP BY CASE WHEN YEAR(products.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(products.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY unit_price_range DESC, launch_cohort ASC
LIMIT 5;
