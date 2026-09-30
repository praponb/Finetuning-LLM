select location, sum(budget) as total_budget
from departments
where location = 'New York' and dept_name in ('HR', 'Marketing', 'Finance')
group by location
order by 1, 2 desc
limit 20
