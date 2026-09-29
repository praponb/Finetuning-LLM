SELECT label, SUM(stock_qty) AS total_stock FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE product_name IS NOT NULL GROUP BY label;
