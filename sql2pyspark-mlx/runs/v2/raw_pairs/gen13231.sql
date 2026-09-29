SELECT s.id, s.opened_date, s.region FROM stores s WHERE s.opened_date > '2025-03-02' AND EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.product_id = 5)
