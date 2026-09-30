with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(salary, 0) salary_rounded,
        cast(is_remote as int) is_remote_flag,
        coalesce(job_title, 'DIRECT') job_title_clean
    from employees
    where hire_date > date '2023-08-31'
      and job_title = 'Analyst'
),
joined as (
    -- Attach employees attributes
    select
        a.id,
        a.full_name,
        a.email,
        a.manager_id,
        a.hire_date,
        a.salary_rounded,
        a.is_remote_flag,
        a.job_title_clean,
        p.salary as employee_salary
    from valid a
    left join employees p
        on a.manager_id = p.id
),
enriched as (
    -- Attach employees attributes
    select e.*, p2.full_name as employee_full_name
    from joined e
    join employees p2
        on e.manager_id = p2.id
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        email,
        hire_date,
        is_remote_flag,
        job_title_clean,
        employee_salary,
        employee_full_name,
        initcap(trim(full_name)) full_name_title
    from enriched
    where manager_id is not null
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        count(employee_salary) over (order by is_remote_flag, id rows between 6 preceding and current row) last7_count_employee_salary,
        max(employee_salary) over (order by is_remote_flag, id rows between 6 preceding and current row) last7_max_employee_salary,
        min(employee_salary) over (order by is_remote_flag, id rows between 6 preceding and current row) last7_min_employee_salary,
        cume_dist() over (order by employee_salary desc nulls first) employee_salary_cume_dist
    from filtered
)
-- Window metrics over several different partitions
select
    id,
    is_remote_flag,
    job_title_clean,
    employee_salary,
    employee_full_name,
    avg(employee_salary_cume_dist) over w_ordered running_avg_employee_salary_cume_dist,
    min(employee_salary_cume_dist) over w_ordered running_min_employee_salary_cume_dist,
    count(employee_salary_cume_dist) over w_ordered running_count_employee_salary_cume_dist,
    last_value(last7_min_employee_salary) ignore nulls over (partition by month(hire_date) order by is_remote_flag, id rows between unbounded preceding and current row) last_known_last7_min_employee_salary
from metrics
window w_ordered as (order by last7_min_employee_salary, id rows between unbounded preceding and current row);
