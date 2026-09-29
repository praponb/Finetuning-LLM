select job_title, row_count
from (
    select job_title, count(*) as row_count
    from employees
    group by job_title
) as agg
where row_count > 9
