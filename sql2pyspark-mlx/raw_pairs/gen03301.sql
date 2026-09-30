with filtered as (
    -- Keep recent, valid rows
    select
        *,
        concat_ws('/', location, dept_name) as location_dept_name_key,
        coalesce(location, 'OTHER') as location_clean
    from departments
    where location not in ('Berlin', 'London')
)
-- Anti-join via a correlated subquery
select s.*
from filtered s
where not exists (
        select 1
        from employees ch
        where ch.dept_id = s.id
          and ch.is_remote
    )
