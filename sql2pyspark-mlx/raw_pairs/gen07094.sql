select i.last_restock, i.quantity, ss.product_id from inventory i left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id where ss.units > 38 and i.store_id = 1;
