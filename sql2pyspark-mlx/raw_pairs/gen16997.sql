select *
from (
    select *, rank() over (partition by dept_name order by budget desc) as dedup_rank
    from departments
) ranked
where dedup_rank = 1;
