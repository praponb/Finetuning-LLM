SELECT id, stock_qty, LEAST(cost, price) AS least_cost_price FROM products;
