with valid as (
    select *, length(dept_name) as dept_name_length
    from departments
    where dept_name is not null
      and location = 'Tokyo'
),
kept as (
    select s.*
    from valid s
    where not exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.dept_id in (16, 27, 33)
        )
),
not_flagged as (
    select s.*
    from kept s
    where not exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and lower(trim(c.email)) = 'jack@startup.io'
        )
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by dept_name, location order by dept_name_length desc nulls first, id) as rn
        from not_flagged
    ) x
    where rn = 1
)
select
    id,
    dept_name,
    location,
    dept_name_length,
    rn
from deduped
order by dept_name_length, location;
