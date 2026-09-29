select id, customer_id, discount, rank() over (partition by customer_id order by discount desc) as discount_rank from orders where store_id = 9 order by id desc;
