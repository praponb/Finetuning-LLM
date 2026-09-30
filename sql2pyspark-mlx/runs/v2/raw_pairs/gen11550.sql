SELECT ss.id, ss.units, p.category, p.cost, s.opened_date FROM store_sales ss RIGHT JOIN products p ON p.id = ss.product_id INNER JOIN stores s ON s.id = ss.store_id;
