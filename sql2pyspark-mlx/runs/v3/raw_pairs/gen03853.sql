SELECT id, brand, stock_qty, price, FIRST_VALUE(price) OVER (PARTITION BY brand ORDER BY stock_qty, id) AS first_price FROM products;
