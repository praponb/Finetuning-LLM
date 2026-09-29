SELECT label, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE stock_qty >= 200 GROUP BY label;
