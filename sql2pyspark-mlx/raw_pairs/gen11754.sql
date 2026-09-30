SELECT label, SUM(stock_qty) AS total_stock FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE ARRAY_CONTAINS(tags, 'bestseller') GROUP BY label ORDER BY total_stock DESC, label;
