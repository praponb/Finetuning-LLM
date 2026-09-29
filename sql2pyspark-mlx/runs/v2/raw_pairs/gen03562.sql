SELECT * FROM (SELECT brand, category, stock_qty FROM products WHERE launch_date BETWEEN '2024-09-01' AND '2025-09-01') PIVOT (COUNT(stock_qty) FOR category IN ('Fashion', 'Electronics'));
