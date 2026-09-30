select i.product_id, s.opened_date, s.sqft from inventory i join stores s on s.id = i.store_id;
