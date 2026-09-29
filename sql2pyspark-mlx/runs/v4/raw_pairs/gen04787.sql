select i.id, i.store_id, i.quantity, s.opened_date, p.supplier_id from inventory i right outer join stores s on i.store_id = s.id inner join products p on i.product_id = p.id
