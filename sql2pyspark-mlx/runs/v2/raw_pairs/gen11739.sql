select order_id, sum(quantity) as total_quantity, dense_rank() over (order by sum(quantity) desc) as quantity_rank from order_items group by order_id;
