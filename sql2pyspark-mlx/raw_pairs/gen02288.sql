select i.id, i.last_restock, i.store_id, ss.units, ss.product_id from inventory i inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id order by id desc
