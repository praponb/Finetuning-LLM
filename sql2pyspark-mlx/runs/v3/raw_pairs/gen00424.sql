select p.id, p.stock_qty, p.launch_date from products p where not exists (select 1 from inventory i where i.product_id = p.id and i.product_id in (2, 10)) order by id desc;
