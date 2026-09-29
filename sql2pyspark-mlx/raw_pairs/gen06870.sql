SELECT id, product_id, CASE WHEN revenue >= 4400 THEN 'High' WHEN revenue >= 600 THEN 'Medium' ELSE 'Low' END revenue_level
FROM store_sales;
