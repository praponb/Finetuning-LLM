SELECT tag, SUM(stock_qty) AS total_stock FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE supplier_id IN (15, 25, 37) GROUP BY tag ORDER BY total_stock DESC, tag;
