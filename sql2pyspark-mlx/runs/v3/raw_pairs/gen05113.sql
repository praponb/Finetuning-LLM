SELECT id, revenue, CAST(units AS STRING) AS units_str
FROM store_sales
WHERE product_id = 10 AND revenue <= 3800;
