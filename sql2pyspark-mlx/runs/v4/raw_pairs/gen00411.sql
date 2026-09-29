SELECT i.product_id, s.opened_date, s.region, s.city FROM inventory i LEFT OUTER JOIN stores s ON i.store_id = s.id ORDER BY product_id;
