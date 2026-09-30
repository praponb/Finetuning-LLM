with base as (
    select *
    from employees
    where level is not null and is_remote
),
is_remote_summary as (
    select is_remote, min(hire_date) as min_hire_date, count(case when job_title = 'Director' then 1 end) as director_count
    from base
    group by is_remote
)
select is_remote, min_hire_date, director_count
from is_remote_summary
order by director_count desc, is_remote
limit 50;
