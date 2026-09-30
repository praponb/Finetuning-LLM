select i.last_restock, i.store_id, i.product_id, p.brand, p.launch_date, p.cost from inventory i inner join products p on p.id = i.product_id;
