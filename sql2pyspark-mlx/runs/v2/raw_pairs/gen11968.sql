select i.id, i.product_id, i.quantity, i.last_restock, s.sqft, s.opened_date, s.store_name from inventory i inner join stores s on i.store_id = s.id where i.product_id in (1, 2) order by id desc;
