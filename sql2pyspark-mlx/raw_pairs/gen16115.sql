select case when location in ('New York', 'Tokyo') then 'tier_1' else 'other' end as location_group, max(budget) as max_budget, round(avg(budget), 2) as avg_budget
from departments
where dept_name = 'HR'
group by case when location in ('New York', 'Tokyo') then 'tier_1' else 'other' end
having count(*) >= 5
order by location_group;
