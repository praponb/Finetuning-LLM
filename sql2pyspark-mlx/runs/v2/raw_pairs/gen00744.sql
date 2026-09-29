select s.id, s.store_name, s.city from stores s where s.id in (select store_id from store_sales where product_id = 8) order by id;
