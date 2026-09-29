with with_windows as (
    select
        *,
        first_value(job_title, true) over w_grp first_known_job_title,
        cume_dist() over (partition by manager_id order by salary desc) salary_cume_dist,
        first_value(level, true) over (partition by year(hire_date), job_title order by salary, id rows between unbounded preceding and current row) first_known_level
    from employees
    window w_grp as (partition by job_title order by hire_date, id rows between unbounded preceding and current row)
),
valid as (
    select
        *,
        date_format(hire_date, 'yyyy-MM') hire_date_label,
        date_format(hire_date, 'MMM yyyy') hire_date_label_2,
        coalesce(level, 'OTHER') level_clean
    from with_windows
    where hire_date >= to_date('2023-07-21')
),
filtered as (
    select *, length(job_title) job_title_length
    from valid
    where hire_date > date '2023-07-08'
      and salary > 135000
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by dept_id order by job_title_length desc nulls last, id) as row_num
        from filtered
    ) x
    where row_num = 1
)
select
    p.dept_name,
    p.location,
    a.level,
    count(*) as num_rows,
    avg(a.salary_cume_dist) as avg_salary_cume_dist
from ranked a
left join departments p on a.dept_id = p.id
group by p.dept_name, p.location, a.level;
