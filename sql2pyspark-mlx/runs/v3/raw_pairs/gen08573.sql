SELECT store_name, region FROM stores WHERE YEAR(opened_date) = 2025 INTERSECT SELECT store_name, region FROM stores WHERE store_name NOT LIKE '%Mall%';
