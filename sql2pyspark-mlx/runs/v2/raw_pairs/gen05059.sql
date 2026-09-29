select location, min(budget) as min_budget, max(budget) - min(budget) as budget_range
from departments
group by location
having budget_range < 33
order by location
