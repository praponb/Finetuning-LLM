select i.id, i.last_restock, s.region, s.sqft, p.cost from inventory i left outer join stores s on i.store_id = s.id right outer join products p on p.id = i.product_id order by cost;
