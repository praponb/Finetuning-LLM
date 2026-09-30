select * from (select id, order_id, quantity, rank() over (partition by order_id order by quantity) as rnk from order_items where product_id is null) as r where rnk <= 3 order by id desc
