select category, sum(cost) as total_cost, sum(sum(cost)) over (order by category) as cumulative_cost
from products
group by category
order by 3 desc, category asc
