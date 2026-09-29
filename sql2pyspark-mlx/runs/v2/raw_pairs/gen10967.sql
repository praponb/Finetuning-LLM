SELECT tags, product_name, category, cost / NULLIF(stock_qty, 0) AS cost_per_stock_qty, ARRAY_CONTAINS(tags, 'bestseller') AS is_bestseller FROM products;
