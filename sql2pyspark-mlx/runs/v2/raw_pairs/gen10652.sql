select * from (select *, row_number() over (partition by store_id order by order_date desc, id) as pos from orders where store_id = 33) as t where pos <= 3 order by 5 asc
