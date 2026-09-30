select i.id, i.product_id, i.store_id, s.region, p.cost from inventory i inner join stores s on i.store_id = s.id inner join products p on i.product_id = p.id order by i.store_id;
