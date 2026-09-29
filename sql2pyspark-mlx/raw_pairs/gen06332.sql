SELECT id, product_id, CASE WHEN units >= 38 THEN 'High' WHEN units >= 9 THEN 'Medium' ELSE 'Low' END AS units_band
FROM store_sales
WHERE revenue > 3000 AND product_id IN (2, 4, 9, 10)
ORDER BY units_band DESC, product_id DESC
