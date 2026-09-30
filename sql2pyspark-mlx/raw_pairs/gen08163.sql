select i.store_id, i.quantity, ss.product_id, ss.units, ss.sale_date from inventory i left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id where ss.units = 29
