with base as (
    -- Basic cleansing and defaults
    select *, cast(is_remote as int) as is_remote_flag
    from employees
    where hire_date > to_date('2023-08-28')
),
base_employees as (
    -- Keep recent, valid rows
    select
        *,
        concat_ws(',', job_title, full_name) as job_title_full_name_key,
        initcap(trim(job_title)) as job_title_title
    from base
    where job_title is not null
),
joined as (
    -- Attach departments attributes
    select
        src.id,
        src.full_name,
        src.dept_id,
        src.manager_id,
        src.hire_date,
        src.is_remote,
        src.is_remote_flag,
        src.job_title_full_name_key,
        d.location,
        d.budget,
        d.dept_name
    from base_employees src
    left join departments d
        on src.dept_id = d.id
    where d.dept_name = 'Finance'
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        full_name,
        dept_id,
        manager_id,
        hire_date,
        is_remote,
        is_remote_flag,
        budget,
        dept_name,
        sum(budget) over (partition by dept_id order by budget, id rows between unbounded preceding and current row) as cumulative_budget,
        lag(budget, 1) over (partition by manager_id order by hire_date, id) as prev_budget
    from joined
),
joined_employees as (
    -- Attach employees attributes
    select s.*, d.full_name as employee_full_name
    from with_windows s
    left join employees d
        on s.manager_id = d.id
)
-- Aggregate with conditional metrics
select
    case when cumulative_budget >= 20638875 then 'HIGH' else 'LOW' end as cumulative_budget_tier,
    count(*) as cnt,
    round(avg(is_remote_flag), 2) as avg_is_remote_flag,
    sort_array(collect_set(dept_name)) as dept_name_list,
    size(collect_set(dept_name)) as num_dept_names,
    round(sum(budget) / nullif(count(case when dept_name = 'Marketing' then 1 end), 0), 2) as budget_per_marketing
from joined_employees
group by rollup (case when cumulative_budget >= 20638875 then 'HIGH' else 'LOW' end)
having num_dept_names > 1
order by cnt
