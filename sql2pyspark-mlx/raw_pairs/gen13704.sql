select i.id, i.product_id, ss.store_id from inventory i left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id order by product_id desc
