select * from (select *, row_number() over (partition by product_id order by quantity, id) as pos from order_items) as ranked where pos = 1 order by id desc, product_id desc;
