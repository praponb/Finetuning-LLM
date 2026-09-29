SELECT i.product_id, i.quantity, s.region, ADD_MONTHS(i.last_restock, 6) AS last_restock_plus_months FROM inventory i LEFT OUTER JOIN stores s ON i.store_id = s.id WHERE i.store_id = 6;
