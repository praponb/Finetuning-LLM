with deduped as (
    select
        id,
        full_name,
        dept_id,
        manager_id,
        hire_date
    from (
        select *, row_number() over (partition by level order by hire_date nulls last, id) as latest_rank
        from employees
    ) ranked
    where latest_rank = 1
)
select
    date_trunc('year', hire_date) as year_start,
    count(*) as event_count,
    count(distinct id) as distinct_ids,
    count(distinct dept_id) as distinct_dept_ids,
    count(distinct dept_id) as distinct_dept_ids_2
from deduped
group by cube (date_trunc('year', hire_date))
