select departments.budget, t.avg_salary, t.l1_count
from departments
inner join (select dept_id, round(avg(salary), 2) as avg_salary, count(case when level = 'L1' then 1 end) as l1_count
    from employees
    where manager_id is not null
    group by dept_id) t on departments.id = t.dept_id
where t.l1_count >= 5
order by t.l1_count desc
