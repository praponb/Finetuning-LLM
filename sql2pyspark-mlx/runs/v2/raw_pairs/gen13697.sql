SELECT brand, cost, product_name, price / NULLIF(stock_qty, 0) AS price_per_stock_qty FROM products WHERE ARRAY_CONTAINS(tags, 'bestseller')
