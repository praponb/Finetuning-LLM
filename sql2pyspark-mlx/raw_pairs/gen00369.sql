select i.id, i.quantity, i.last_restock, i.product_id, s.sqft, s.opened_date, s.store_name from inventory i inner join stores s on i.store_id = s.id;
