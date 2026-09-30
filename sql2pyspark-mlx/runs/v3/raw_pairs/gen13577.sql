select location, sum(budget) as total_budget, sum(sum(budget)) over (order by location) as cumulative_budget
from departments
group by location
order by cumulative_budget desc, location
limit 10;
