select s.* from products s where exists (select 1 from inventory ch where ch.product_id = s.id and ch.last_restock >= s.launch_date)
