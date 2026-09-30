select i.id, i.quantity, i.store_id, s.sqft, p.cost, p.category from inventory i right join stores s on i.store_id = s.id join products p on i.product_id = p.id where p.cost >= 260 order by 5;
