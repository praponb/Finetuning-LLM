with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        length(job_title) job_title_length,
        coalesce(level, 'DIRECT') resolved_level
    from employees
    where manager_id is not null
      and manager_id in (7, 9, 20, 39)
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        avg(job_title_length) over w_running running_avg_job_title_length,
        min(job_title_length) over w_running running_min_job_title_length,
        max(job_title_length) over w_running running_max_job_title_length,
        count(job_title_length) over w_running running_count_job_title_length,
        first_value(level, true) over w_ordered first_known_level
    from valid
    where salary between 117000 and 140000
    window w_running as (partition by level order by hire_date, id rows between unbounded preceding and current row), w_ordered as (order by salary, id rows between unbounded preceding and current row)
),
metrics_employees as (
    -- Running and moving aggregates
    select
        id,
        email,
        dept_id,
        hire_date,
        running_min_job_title_length,
        running_max_job_title_length,
        running_count_job_title_length,
        last_value(running_count_job_title_length) ignore nulls over w_part last_known_running_count_job_title_length,
        lag(job_title_length, 2) over (partition by dept_id order by salary, id) prev_job_title_length,
        avg(salary) over w_hist salary_avg_5_prior,
        cume_dist() over (partition by dept_id order by running_max_job_title_length desc nulls last) running_max_job_title_length_cume_dist
    from metrics
    window w_part as (partition by manager_id, job_title order by job_title_length, id rows between unbounded preceding and current row), w_hist as (order by running_avg_job_title_length, id rows between 5 preceding and 1 preceding)
),
metrics_employees_2 as (
    -- Ranking and gap features
    select
        id,
        email,
        dept_id,
        hire_date,
        running_min_job_title_length,
        running_count_job_title_length,
        last_known_running_count_job_title_length,
        prev_job_title_length,
        min(prev_job_title_length) over (partition by dept_id) dept_id_min_prev_job_title_length,
        sum(last_known_running_count_job_title_length) over (partition by dept_id order by running_count_job_title_length, id rows between unbounded preceding and current row) running_last_known_running_count_job_title_length
    from metrics_employees
),
scored as (
    -- Running and moving aggregates
    select
        *,
        min(prev_job_title_length) over w_part running_min_prev_job_title_length,
        sum(prev_job_title_length) over w_part running_sum_prev_job_title_length,
        max(prev_job_title_length) over w_part running_max_prev_job_title_length,
        last_value(running_last_known_running_count_job_title_length) ignore nulls over w_recent last_known_running_last_known_running_count_job_title_length
    from metrics_employees_2
    window w_part as (partition by dept_id order by running_count_job_title_length, id rows between unbounded preceding and current row), w_recent as (partition by dept_id order by running_min_job_title_length, id rows between unbounded preceding and current row)
),
base as (
    -- Basic cleansing and defaults
    select *, ceil(running_min_prev_job_title_length) running_min_prev_job_title_length_whole
    from scored
    where hire_date >= date '2023-05-16'
      and running_max_prev_job_title_length is not null
)
-- Join a dimension and aggregate
select
    dim.dept_name,
    dim.location,
    count(*) as rows_total,
    min(src.last_known_running_count_job_title_length) as min_last_known_running_count_job_title_length,
    max(src.dept_id_min_prev_job_title_length) as max_dept_id_min_prev_job_title_length,
    grouping_id() as gid
from base src
join departments dim on src.dept_id = dim.id
group by grouping sets ((dim.dept_name, dim.location), (dim.dept_name), ())
order by location;
