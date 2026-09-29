select i.last_restock, p.launch_date, p.stock_qty, s.city, s.sqft from inventory i inner join products p on i.product_id = p.id left join stores s on s.id = i.store_id where p.price <= 740;
