select i.id, i.store_id, i.product_id, s.sqft, s.opened_date from inventory i left outer join stores s on s.id = i.store_id where i.store_id in (1, 2, 4, 5);
