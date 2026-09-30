select category, sum(case when category = 'Home' then cost else 0 end) as home_cost
from products
group by category
having sum(case when category = 'Home' then cost else 0 end) < 21
