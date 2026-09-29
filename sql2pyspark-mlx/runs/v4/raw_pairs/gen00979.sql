SELECT ss.id, ss.product_id FROM store_sales ss WHERE ss.sale_date BETWEEN '2023-08-01' AND '2024-07-26' AND ss.product_id IN (SELECT id FROM products WHERE brand IN ('Initech', 'Umbrella'));
