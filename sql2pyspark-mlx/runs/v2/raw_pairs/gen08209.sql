select i.id, i.product_id, ss.sale_date, weekofyear(i.last_restock) as last_restock_weekofyear from inventory i left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id;
