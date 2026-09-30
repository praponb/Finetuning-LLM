select p.launch_date, p.price, i.quantity, i.store_id from products p left join inventory i on i.product_id = p.id order by launch_date asc
