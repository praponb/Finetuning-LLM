SELECT tag, SUM(stock_qty) AS total_stock FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE supplier_id IN (4, 15, 24) GROUP BY tag ORDER BY total_stock DESC, tag
