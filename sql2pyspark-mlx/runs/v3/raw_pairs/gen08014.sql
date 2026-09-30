select i.last_restock, p.supplier_id, p.category, s.opened_date, s.region from inventory i left join products p on p.id = i.product_id inner join stores s on i.store_id = s.id;
