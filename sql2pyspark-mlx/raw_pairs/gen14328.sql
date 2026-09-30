with cleaned as (
    select
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        is_remote,
        level,
        concat_ws(',', email, level) as email_level_key,
        length(full_name) as full_name_length
    from employees
    where hire_date > to_date('2023-02-16')
      and dept_id in (15, 29, 38)
),
enriched as (
    select
        a.id,
        a.full_name,
        a.dept_id,
        a.job_title,
        a.is_remote,
        a.level,
        a.email_level_key,
        a.full_name_length,
        d.dept_name,
        d.location
    from cleaned a
    inner join departments d
        on a.dept_id = d.id
),
enriched_employees as (
    select
        e.id,
        e.full_name,
        e.dept_id,
        e.job_title,
        e.full_name_length,
        e.location,
        d.dept_name as department_dept_name,
        d.location as department_location
    from enriched e
    left join departments d
        on e.dept_id = d.id
),
joined as (
    select
        src.id,
        src.full_name,
        src.dept_id,
        src.full_name_length,
        src.department_dept_name,
        src.department_location,
        b.location as department_location_2
    from enriched_employees src
    left join departments b
        on src.dept_id = b.id
)
select
    department_dept_name,
    department_location_2,
    round(sum(full_name_length) / nullif(count(case when department_location = 'Tokyo' then 1 end), 0), 2) as full_name_length_per_tokyo,
    count(case when department_location = 'Bangkok' then 1 end) as bangkok_count,
    grouping(department_dept_name) as gid
from joined
group by grouping sets ((department_dept_name, department_location_2), (department_dept_name), ());
