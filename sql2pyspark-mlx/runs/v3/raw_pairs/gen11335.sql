select case when d.budget >= 4242000 then 'high' when d.budget >= 1241000 then 'mid' else 'low' end as budget_bucket, min(e.salary) as min_salary
from employees e
join departments d on d.id = e.dept_id
where d.dept_name is null
group by case when d.budget >= 4242000 then 'high' when d.budget >= 1241000 then 'mid' else 'low' end
having min(e.salary) > 134000
order by min_salary desc
