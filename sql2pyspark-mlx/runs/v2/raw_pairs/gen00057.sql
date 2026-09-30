select i.id, i.quantity, i.product_id, s.region, s.opened_date, p.price from inventory i left join stores s on i.store_id = s.id right join products p on p.id = i.product_id where i.store_id = 1;
