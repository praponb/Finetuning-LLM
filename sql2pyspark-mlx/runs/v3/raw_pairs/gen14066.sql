SELECT *, ARRAY_JOIN(tags, '|') tags_text, LEAST(cost, stock_qty) least_cost_stock_qty
FROM products
