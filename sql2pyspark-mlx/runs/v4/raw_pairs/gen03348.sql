with deduped as (
    -- QUALIFY-style window filter
    select *
    from departments
    qualify row_number() over (partition by location order by budget desc nulls last, id) = 1
),
with_last as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.employee_hire_date,
        x.employee_salary
    from deduped s
    join lateral (
            select c.hire_date as employee_hire_date, c.salary as employee_salary
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) x on true
),
scored as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by dept_name, location order by budget desc) as budget_percent_rank,
        first_value(budget) ignore nulls over w_recent as first_known_budget,
        lag(employee_salary, 2, 0) over w_hist as prev_employee_salary
    from with_last
    window w_recent as (partition by dept_name order by employee_hire_date, id rows between unbounded preceding and current row), w_hist as (partition by dept_name order by employee_salary, id)
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from scored s
    where exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.hire_date >= s.employee_hire_date
        )
),
deduped_departments as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by dept_name, location order by first_known_budget desc nulls first, id) as latest_rank
        from kept
    ) x
    where latest_rank = 1
),
matched_keys as (
    -- Set intersection on keys
    select dept_name, id
    from deduped_departments
    intersect
    select dept_name, id
    from departments
    where budget > 1178000
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped_departments s
    join matched_keys k on s.dept_name = k.dept_name and s.id = k.id
)
-- Grouped metrics
select
    date_trunc('week', employee_hire_date) as week_start,
    sum(case when location = 'Singapore' then budget else 0 end) as singapore_budget,
    max(budget) filter (where dept_name = 'Engineering') as engineering_max_budget,
    count(distinct location) as distinct_locations
from verified
group by date_trunc('week', employee_hire_date)
order by engineering_max_budget nulls first;
