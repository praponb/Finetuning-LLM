SELECT MIN(sqft) AS min_sqft FROM stores WHERE store_name LIKE '%Exp%' AND store_name LIKE '%Rive%' AND opened_date < '2024-10-12';
