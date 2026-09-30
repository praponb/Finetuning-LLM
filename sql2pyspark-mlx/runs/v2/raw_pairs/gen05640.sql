SELECT label, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE stock_qty < 347 GROUP BY label ORDER BY product_count DESC, label
