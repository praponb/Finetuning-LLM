SELECT label, SUM(stock_qty) AS total_stock FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE cost <= 720 GROUP BY label;
