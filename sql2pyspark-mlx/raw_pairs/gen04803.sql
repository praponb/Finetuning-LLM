select i.last_restock, p.stock_qty, s.opened_date from inventory i join products p on p.id = i.product_id join stores s on i.store_id = s.id where p.price > 710 and s.city = 'Tokyo';
