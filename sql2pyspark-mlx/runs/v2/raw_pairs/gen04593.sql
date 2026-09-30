SELECT SUM(CASE WHEN brand = 'Acme' THEN stock_qty ELSE 0 END) acme_stock_qty, MAX(cost) max_cost FROM products WHERE LENGTH(product_name) > 6 AND launch_date >= '2025-01-22' AND stock_qty <= 327;
