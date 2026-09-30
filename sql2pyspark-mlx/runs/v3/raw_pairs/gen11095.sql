select i.id, i.product_id, s.store_name, s.opened_date, p.launch_date from inventory i left outer join stores s on i.store_id = s.id join products p on i.product_id = p.id;
