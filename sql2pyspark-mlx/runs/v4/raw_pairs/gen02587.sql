with metrics as (
    -- Running and moving aggregates
    select
        *,
        lead(salary, 2, 0) over (partition by level, manager_id order by hire_date, id) as next_salary,
        round(salary / sum(salary) over (partition by job_title, level), 4) as salary_share,
        row_number() over (order by salary, id) as seq_no
    from employees
),
matched_keys as (
    -- Set difference on keys
    select job_title
    from metrics
    except all
    select job_title
    from employees
    where full_name is not null
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    inner join matched_keys k on s.job_title = k.job_title
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        cume_dist() over (order by salary desc) as salary_cume_dist,
        round(salary / sum(salary) over (partition by job_title), 4) as salary_share_2,
        percent_rank() over (order by salary) as salary_percent_rank,
        rank() over (partition by dept_id order by salary) as salary_rank,
        percent_rank() over (order by salary_share) as salary_share_percent_rank
    from reconciled
)
-- Ranking and gap features
select
    *,
    dense_rank() over (partition by level order by next_salary desc nulls first) as next_salary_dense_rank,
    rank() over w_ordered as salary_share_2_rank
from with_windows
window w_ordered as (partition by level order by salary_share_2 desc nulls first)
