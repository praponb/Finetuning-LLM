select location, round(avg(budget), 2) as avg_budget, max(budget) - min(budget) as budget_range
from departments
group by location
order by location;
