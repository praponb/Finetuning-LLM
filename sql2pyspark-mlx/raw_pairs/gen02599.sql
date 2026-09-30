with base as (
    -- Basic cleansing and defaults
    select
        *,
        datediff(current_date(), hire_date) as days_since_hire_date,
        lower(job_title) as job_title_lower
    from employees
    where hire_date > date '2023-02-24'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (order by hire_date, id) as row_seq,
        sum(days_since_hire_date) over (partition by dept_id order by hire_date, id rows between unbounded preceding and current row) as cumulative_days_since_hire_date,
        row_number() over (order by days_since_hire_date desc, id) as seq_no
    from base
    where length(full_name) < 14
),
with_windows_employees as (
    -- Ranking and gap features
    select
        id,
        full_name,
        email,
        dept_id,
        salary,
        hire_date,
        job_title_lower,
        seq_no,
        avg(cumulative_days_since_hire_date) over w_part as running_avg_cumulative_days_since_hire_date,
        min(cumulative_days_since_hire_date) over w_part as running_min_cumulative_days_since_hire_date,
        sum(cumulative_days_since_hire_date) over w_part as running_sum_cumulative_days_since_hire_date,
        count(cumulative_days_since_hire_date) over w_part as running_count_cumulative_days_since_hire_date,
        sum(row_seq) over (partition by year(hire_date) order by seq_no, id rows between unbounded preceding and current row) as running_row_seq
    from with_windows
    window w_part as (partition by level, dept_id order by cumulative_days_since_hire_date, id rows between unbounded preceding and current row)
),
with_dims as (
    -- Attach departments attributes
    select s.*, p.budget
    from with_windows_employees s
    left join departments p
        on s.dept_id = p.id
)
select
    id,
    full_name,
    dept_id,
    running_sum_cumulative_days_since_hire_date,
    running_count_cumulative_days_since_hire_date,
    running_row_seq,
    budget,
    case when full_name rlike '^(Olivia Garcia|Jack Taylor)$' then 'PRIORITY' else 'CORE' end as full_name_zone
from with_dims
order by full_name_zone desc
