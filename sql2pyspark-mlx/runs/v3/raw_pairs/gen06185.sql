SELECT tag_name, SUM(stock_qty) AS total_stock FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE product_name LIKE '%Yog%' GROUP BY tag_name ORDER BY total_stock DESC, tag_name
