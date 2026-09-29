SELECT tag_name, SUM(stock_qty) AS total_stock FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE brand IN ('Globex', 'Umbrella') GROUP BY tag_name
