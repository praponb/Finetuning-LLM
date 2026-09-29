select i.id, i.product_id, p.price, p.brand, s.city, s.rating from inventory i right join products p on i.product_id = p.id full outer join suppliers s on p.supplier_id = s.id order by s.rating
