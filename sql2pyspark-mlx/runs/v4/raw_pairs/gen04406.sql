SELECT p.brand, QUARTER(p.launch_date) AS launch_quarter, COUNT(*) AS store_sale_count
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
LEFT OUTER JOIN products p ON ss.product_id = p.id
GROUP BY p.brand, QUARTER(p.launch_date)
ORDER BY store_sale_count DESC, brand, launch_quarter
LIMIT 20;
