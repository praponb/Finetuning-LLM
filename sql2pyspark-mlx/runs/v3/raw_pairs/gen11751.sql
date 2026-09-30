SELECT i.id, i.quantity FROM inventory i WHERE i.store_id NOT IN (SELECT id FROM stores WHERE opened_date BETWEEN '2023-07-01' AND '2023-08-01') ORDER BY i.id ASC;
