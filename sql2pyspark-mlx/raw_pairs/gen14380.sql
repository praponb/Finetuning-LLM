select d.dept_name, t.avg_salary, t.stddev_salary
from departments d
inner join (select dept_id, round(avg(salary), 2) as avg_salary, round(stddev(salary), 2) as stddev_salary
    from employees
    where salary >= 119000
    group by dept_id) t on d.id = t.dept_id
order by dept_name;
