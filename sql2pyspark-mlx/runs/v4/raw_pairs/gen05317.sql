SELECT id, category, price, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price, id) AS row_num FROM products WHERE SIZE(tags) >= 2;
