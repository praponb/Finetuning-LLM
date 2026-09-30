SELECT product_id, units, CAST(units AS STRING) AS units_str
FROM store_sales
ORDER BY product_id DESC
