SELECT region, city FROM stores EXCEPT ALL SELECT region, city FROM stores WHERE store_name LIKE '%Cen%'
