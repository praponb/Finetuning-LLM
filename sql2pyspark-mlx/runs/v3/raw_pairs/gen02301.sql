select i.quantity, i.store_id, i.last_restock, p.launch_date from inventory i left join products p on i.product_id = p.id where upper(p.product_name) like '%ATE%';
