SELECT product_id FROM inventory INTERSECT ALL SELECT product_id FROM inventory WHERE quantity < 100;
