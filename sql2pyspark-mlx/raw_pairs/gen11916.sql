SELECT i.store_id, p.price, s.store_name, ROUND(s.sqft, 2) AS sqft_rounded FROM inventory i LEFT OUTER JOIN products p ON i.product_id = p.id JOIN stores s ON s.id = i.store_id;
