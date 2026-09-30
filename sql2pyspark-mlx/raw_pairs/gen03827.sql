select level, month(hire_date) as hire_month, count(distinct salary) as unique_managers
from employees
where salary between 68000 and 94000 or salary <= 120000
group by level, month(hire_date)
having unique_managers > 4
order by unique_managers desc;
