select i.id, i.last_restock, i.product_id, i.store_id, ss.product_id as store_sale_product_id from inventory i inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id;
