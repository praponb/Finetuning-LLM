select i.id, i.last_restock, ss.revenue, ss.sale_date from inventory i left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id where ss.store_id = 1;
