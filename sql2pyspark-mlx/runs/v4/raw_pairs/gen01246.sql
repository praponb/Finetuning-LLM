SELECT i.product_id, i.quantity, s.opened_date, s.sqft, p.cost FROM inventory i JOIN stores s ON i.store_id = s.id FULL OUTER JOIN products p ON p.id = i.product_id;
