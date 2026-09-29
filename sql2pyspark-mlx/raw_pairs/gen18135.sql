select p.price, p.product_name, i.store_id, i.product_id from products p left join inventory i on p.id = i.product_id
