SELECT CASE WHEN stock_qty >= 371 THEN 'high' WHEN stock_qty >= 108 THEN 'mid' ELSE 'low' END AS stock_qty_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values
FROM products
WHERE product_name IS NOT NULL AND product_name LIKE '%Chair%' AND category = 'Sports'
GROUP BY 1
ORDER BY brand_values DESC, stock_qty_bucket
