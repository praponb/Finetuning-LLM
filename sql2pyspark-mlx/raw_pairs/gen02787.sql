SELECT p.launch_date, p.product_name, s.rating, s.country FROM products p INNER JOIN suppliers s ON s.id = p.supplier_id WHERE YEAR(p.launch_date) = 2023 AND s.country NOT IN ('DE', 'UK');
