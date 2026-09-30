select i.product_id, ss.store_id, ss.sale_date from inventory i join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id order by store_id desc;
