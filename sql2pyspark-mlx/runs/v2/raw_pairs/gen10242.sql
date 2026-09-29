SELECT tag, SUM(stock_qty) AS total_stock FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE brand IN ('Stark', 'Acme') GROUP BY tag;
