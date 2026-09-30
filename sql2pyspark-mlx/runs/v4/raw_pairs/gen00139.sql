select id, quantity, rank() over (order by quantity desc) as quantity_rank from order_items
