select i.id, i.store_id, p.product_name, p.price, s.city from inventory i full outer join products p on i.product_id = p.id inner join suppliers s on p.supplier_id = s.id
