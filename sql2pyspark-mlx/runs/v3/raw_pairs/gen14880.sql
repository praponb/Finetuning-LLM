SELECT * FROM (SELECT category, brand, cost FROM products WHERE stock_qty = 241)
PIVOT (COUNT(cost) FOR brand IN ('Acme', 'Globex'));
