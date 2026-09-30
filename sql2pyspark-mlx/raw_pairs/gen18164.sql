SELECT i.store_id, i.product_id, s.city, s.opened_date, s.region FROM inventory i RIGHT OUTER JOIN stores s ON i.store_id = s.id ORDER BY city DESC;
