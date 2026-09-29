with ranked_employees as (
    select *, rank() over (partition by dept_id order by hire_date desc) as row_num
    from employees
)
select *
from ranked_employees
where row_num <= 3;
