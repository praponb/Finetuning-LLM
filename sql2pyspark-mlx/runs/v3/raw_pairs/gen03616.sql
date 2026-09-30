SELECT i.id, i.store_id FROM inventory i WHERE i.product_id IN (SELECT id FROM products WHERE category IN ('Grocery', 'Books')) ORDER BY store_id DESC, i.id DESC;
