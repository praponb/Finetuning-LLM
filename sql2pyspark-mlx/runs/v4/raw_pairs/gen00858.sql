with base as (
    -- Filter the raw rows and derive helper columns
    select *, datediff(current_date(), hire_date) as days_since_hire_date
    from employees
    where hire_date > date '2023-04-08'
),
joined as (
    -- Attach departments attributes
    select
        s.*,
        d.dept_name,
        d.budget,
        d.location
    from base s
    join departments d
        on s.dept_id = d.id
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from joined
    qualify row_number() over (partition by level order by hire_date nulls first, id) = 1
)
-- Running and moving aggregates
select
    id,
    full_name,
    email,
    dept_id,
    manager_id,
    job_title,
    salary,
    hire_date,
    is_remote,
    level,
    days_since_hire_date,
    dept_name,
    budget,
    location,
    rank() over (partition by location order by salary desc) as salary_rank,
    sum(budget) over (order by salary, id rows between unbounded preceding and current row) as cumulative_budget,
    last_value(location) ignore nulls over (partition by dept_id order by budget, id rows between unbounded preceding and current row) as last_known_location,
    avg(budget) over (partition by job_title, location order by hire_date, id rows between 5 preceding and 1 preceding) as budget_avg_5_prior,
    round(budget / sum(budget) over (partition by location), 4) as budget_share
from ranked;
