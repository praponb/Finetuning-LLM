select id, units, store_id from store_sales where product_id = 5 intersect select id, units, store_id from store_sales where revenue is not null order by units desc;
