select * from (select id, order_id, amount, row_number() over (partition by order_id order by amount desc, id) as row_num from payments) as r where row_num = 1
