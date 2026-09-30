select id, quantity, rank() over (order by quantity) rnk from order_items
