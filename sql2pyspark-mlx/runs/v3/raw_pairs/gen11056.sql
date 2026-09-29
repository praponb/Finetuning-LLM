select location, count(budget) as budget_count
from departments
where dept_name in ('Legal', 'Sales', 'Marketing')
group by location
having count(budget) >= 2;
