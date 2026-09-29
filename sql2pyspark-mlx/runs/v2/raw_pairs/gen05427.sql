select store_id, sum(quantity) as total_quantity, rank() over (order by sum(quantity) desc) as rnk from inventory group by store_id
