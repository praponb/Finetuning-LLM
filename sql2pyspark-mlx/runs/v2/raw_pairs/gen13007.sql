select id, order_id, quantity, dense_rank() over (partition by order_id order by quantity desc) as dense_rnk from order_items;
