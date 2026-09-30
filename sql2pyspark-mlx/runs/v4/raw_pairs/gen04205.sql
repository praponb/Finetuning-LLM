with remaining_keys as (
    -- Set intersection on keys
    select dept_id
    from employees
    intersect
    select dept_id
    from employees
    where dept_id = 36
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from employees s
    inner join remaining_keys k on s.dept_id = k.dept_id
)
-- Multi-level aggregation
select
    job_title,
    count(case when job_title = 'Intern' then 1 end) as intern_count,
    count(*) filter (where level = 'L4') as l4_rows,
    count(case when level = 'L2' then 1 end) as l2_count,
    round(sum(salary) / nullif(count(case when level = 'L5' then 1 end), 0), 2) as salary_per_l5,
    approx_percentile(salary, 0.9) as salary_p90
from reconciled
group by 1
order by intern_count
