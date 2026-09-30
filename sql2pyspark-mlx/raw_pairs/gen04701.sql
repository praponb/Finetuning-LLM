with not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from departments s
    where not exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.full_name not like 'E%'
        )
),
latest_match as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.employee_hire_date,
        top1.employee_level
    from not_flagged s
    left join lateral (
            select c.hire_date as employee_hire_date, c.level as employee_level
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) top1 on true
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        dept_name,
        employee_hire_date,
        employee_level,
        first_value(location, true) over (partition by employee_level order by budget, id rows between unbounded preceding and current row) as first_known_location,
        count(budget) over (partition by month(employee_hire_date), employee_level) as employee_hire_date_month_count_budget
    from latest_match
    where location in ('Tokyo', 'Berlin', 'Chiang Mai')
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        count(employee_hire_date_month_count_budget) over (partition by employee_level, dept_name order by employee_hire_date_month_count_budget, id rows between unbounded preceding and current row) as running_count_employee_hire_date_month_count_budget,
        max(employee_hire_date_month_count_budget) over (partition by employee_level, dept_name order by employee_hire_date_month_count_budget, id rows between unbounded preceding and current row) as running_max_employee_hire_date_month_count_budget,
        sum(employee_hire_date_month_count_budget) over (partition by employee_level, dept_name order by employee_hire_date_month_count_budget, id rows between unbounded preceding and current row) as running_sum_employee_hire_date_month_count_budget,
        round(employee_hire_date_month_count_budget / sum(employee_hire_date_month_count_budget) over (partition by dept_name), 4) as employee_hire_date_month_count_budget_share
    from metrics
),
deduped as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by dept_name order by employee_hire_date_month_count_budget_share nulls first) as rank_in_group
    from scored
    qualify rank_in_group = 1
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        round(running_sum_employee_hire_date_month_count_budget / sum(running_sum_employee_hire_date_month_count_budget) over (partition by dept_name, employee_level), 4) as running_sum_employee_hire_date_month_count_budget_share,
        max(running_sum_employee_hire_date_month_count_budget) over w_part as dept_name_max_running_sum_employee_hire_date_month_count_budget,
        ntile(3) over (partition by employee_level order by employee_hire_date_month_count_budget_share, id) as employee_hire_date_month_count_budget_share_bucket,
        last_value(dept_name, true) over (partition by year(employee_hire_date) order by running_sum_employee_hire_date_month_count_budget, id rows between unbounded preceding and current row) as last_known_dept_name,
        ntile(4) over w_ordered as employee_hire_date_month_count_budget_bucket
    from deduped
    window w_part as (partition by dept_name, employee_level), w_ordered as (partition by employee_level order by employee_hire_date_month_count_budget desc, id)
),
scored_departments as (
    -- Ranking and gap features
    select
        id,
        dept_name,
        employee_hire_date,
        employee_level,
        running_count_employee_hire_date_month_count_budget,
        running_max_employee_hire_date_month_count_budget,
        rank_in_group,
        running_sum_employee_hire_date_month_count_budget_share,
        dept_name_max_running_sum_employee_hire_date_month_count_budget,
        employee_hire_date_month_count_budget_share_bucket,
        employee_hire_date_month_count_budget_bucket,
        ntile(4) over (partition by dept_name order by running_max_employee_hire_date_month_count_budget desc, id) as running_max_employee_hire_date_month_count_budget_bucket,
        avg(employee_hire_date_month_count_budget_share_bucket) over (partition by employee_level order by running_sum_employee_hire_date_month_count_budget, id rows between 2 preceding and 1 preceding) as employee_hire_date_month_count_budget_share_bucket_avg_2_prior,
        min(employee_hire_date_month_count_budget_share_bucket) over (partition by dept_name, employee_level) as dept_name_min_employee_hire_date_month_count_budget_share_bucket
    from with_windows
)
select
    id,
    employee_hire_date,
    running_count_employee_hire_date_month_count_budget,
    running_max_employee_hire_date_month_count_budget,
    running_sum_employee_hire_date_month_count_budget_share,
    dept_name_max_running_sum_employee_hire_date_month_count_budget,
    employee_hire_date_month_count_budget_share_bucket,
    employee_hire_date_month_count_budget_bucket,
    running_max_employee_hire_date_month_count_budget_bucket
from scored_departments
order by employee_hire_date_month_count_budget_share_bucket nulls last, id desc
