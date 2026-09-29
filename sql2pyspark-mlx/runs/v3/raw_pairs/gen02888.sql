SELECT id, category, price, cost + stock_qty AS cost_plus_stock_qty, ROUND(cost / price, 2) AS margin FROM products WHERE supplier_id IS NULL OR cost BETWEEN 370 AND 440 ORDER BY id DESC;
