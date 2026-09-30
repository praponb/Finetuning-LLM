SELECT tag, SUM(stock_qty) AS total_stock FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE brand NOT IN ('Globex', 'Acme') GROUP BY tag ORDER BY total_stock DESC, tag
