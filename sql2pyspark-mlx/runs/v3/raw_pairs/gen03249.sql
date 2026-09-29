select p.supplier_id, min(i.last_restock) as first_last_restock, count(*) as num_inventory from inventory i left join products p on i.product_id = p.id group by p.supplier_id;
