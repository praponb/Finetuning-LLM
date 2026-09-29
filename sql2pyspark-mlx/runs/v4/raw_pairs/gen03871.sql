SELECT cost FROM products UNION ALL SELECT revenue FROM store_sales WHERE sale_date BETWEEN '2023-06-01' AND '2024-03-01' ORDER BY cost DESC;
