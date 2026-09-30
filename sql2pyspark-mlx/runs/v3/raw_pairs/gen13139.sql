SELECT i.id, i.store_id, s.city, s.store_name, p.price, p.stock_qty FROM inventory i JOIN stores s ON i.store_id = s.id JOIN products p ON i.product_id = p.id WHERE YEAR(s.opened_date) >= 2025
