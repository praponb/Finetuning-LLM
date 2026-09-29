SELECT
    CASE WHEN products.stock_qty >= 369 THEN 'high' WHEN products.stock_qty >= 161 THEN 'mid' ELSE 'low' END AS stock_qty_bucket,
    products.brand,
    MAX(inventory.last_restock) AS latest_last_restock,
    ROUND(AVG(inventory.quantity), 2) AS avg_quantity
FROM inventory
INNER JOIN products ON inventory.product_id = products.id
WHERE products.brand = 'Globex'
GROUP BY CASE WHEN products.stock_qty >= 369 THEN 'high' WHEN products.stock_qty >= 161 THEN 'mid' ELSE 'low' END, products.brand
ORDER BY avg_quantity
