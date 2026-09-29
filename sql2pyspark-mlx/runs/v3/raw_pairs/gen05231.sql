select d.location, d.dept_name, t.unique_depts
from departments d
left join (select dept_id, count(distinct salary) as unique_depts
    from employees
    group by dept_id) t on d.id = t.dept_id
order by unique_depts desc
