SELECT supplier_id, LEAST(price, cost) AS least_price_cost, COALESCE(stock_qty, 0) AS stock_qty_filled FROM products ORDER BY 3 DESC;
