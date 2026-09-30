select hire_quarter, total_count
from (
    select quarter(hire_date) as hire_quarter, count(*) as total_count
    from employees
    group by quarter(hire_date)
) as t
where total_count < 6;
