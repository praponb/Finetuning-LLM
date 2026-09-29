select category, avg(cost) as avg_cost
from products
where category = 'Home' and array_contains(tags, 'new')
group by category
having avg_cost >= 280
order by category
limit 20
