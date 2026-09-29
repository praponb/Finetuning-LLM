with numbered as (
    select *, rank() over (partition by level order by hire_date desc) as rnk
    from employees
    where salary between 108000 and 145000
)
select *
from numbered
where rnk = 1;
