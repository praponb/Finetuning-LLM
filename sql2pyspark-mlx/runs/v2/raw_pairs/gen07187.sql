SELECT id, revenue, store_id FROM store_sales WHERE revenue = (SELECT MIN(revenue) FROM store_sales WHERE sale_date > '2024-04-08');
