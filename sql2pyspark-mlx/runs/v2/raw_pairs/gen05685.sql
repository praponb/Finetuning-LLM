SELECT * FROM (SELECT brand, category, cost FROM products WHERE launch_date <= '2024-06-11') PIVOT (MAX(cost) FOR category IN ('Books', 'Sports', 'Home', 'Electronics')) ORDER BY brand
