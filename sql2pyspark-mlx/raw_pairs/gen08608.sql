SELECT id, revenue, CASE WHEN revenue >= 3900 THEN 'High' WHEN revenue >= 1000 THEN 'Medium' ELSE 'Low' END revenue_tier
FROM store_sales
WHERE store_id IN (1, 2, 3) OR product_id = 6
