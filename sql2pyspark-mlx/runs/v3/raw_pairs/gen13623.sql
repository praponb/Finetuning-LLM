SELECT tag_name, SUM(stock_qty) AS total_stock FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name GROUP BY tag_name
