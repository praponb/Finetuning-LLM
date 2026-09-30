SELECT i.id, i.product_id, s.opened_date FROM inventory i INNER JOIN stores s ON i.store_id = s.id WHERE MONTH(i.last_restock) IN (4, 7, 10) ORDER BY id DESC
