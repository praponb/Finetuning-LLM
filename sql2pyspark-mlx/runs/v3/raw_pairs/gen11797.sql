select hire_month, num_employees
from (
    select date_format(hire_date, 'yyyy-MM') as hire_month, count(*) as num_employees
    from employees
    where level != 'L3'
    group by date_format(hire_date, 'yyyy-MM')
) as x
where num_employees < 10
order by num_employees desc;
