select store_id, sum(quantity) as total_quantity, sum(sum(quantity)) over (order by store_id) as cumulative_quantity
from inventory
group by store_id
order by total_quantity desc;
