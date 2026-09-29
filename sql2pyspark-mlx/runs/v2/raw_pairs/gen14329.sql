with numbered as (
    select *, rank() over (partition by dept_name order by budget) rn
    from departments
    where dept_name in ('Marketing', 'Engineering')
)
select *
from numbered
where rn <= 3
order by dept_name
