SELECT store_id, id FROM orders INTERSECT ALL SELECT store_id, id FROM orders WHERE discount >= 0.27;
