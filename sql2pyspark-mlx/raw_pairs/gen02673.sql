with scored as (
    -- Ranking and gap features
    select
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        level,
        max(salary) over (partition by level order by hire_date asc, id asc rows between 6 preceding and current row) as last7_max_salary,
        sum(salary) over (partition by level order by hire_date asc, id asc rows between 6 preceding and current row) as last7_sum_salary,
        count(salary) over (partition by level order by hire_date asc, id asc rows between 6 preceding and current row) as last7_count_salary
    from employees
    where hire_date > '2024-12-01'
),
joined as (
    -- Attach departments attributes
    select
        e.id,
        e.dept_id,
        e.manager_id,
        e.hire_date,
        b.dept_name
    from scored e
    inner join departments b
        on e.dept_id = b.id
),
with_dims as (
    -- Attach departments attributes
    select
        e.*,
        p.dept_name as department_dept_name,
        p.location,
        p.budget
    from joined e
    inner join departments p
        on e.dept_id = p.id
),
ranked as (
    -- Keep one row per key (latest first)
    select
        id,
        dept_id,
        hire_date,
        dept_name,
        department_dept_name,
        location
    from with_dims
    qualify row_number() over (partition by dept_name order by budget desc, id asc) = 1
)
-- Multi-level aggregation
select
    date_trunc('quarter', hire_date) as quarter_start,
    count(*) filter (where dept_name = 'Finance') as finance_rows,
    count(*) filter (where department_dept_name = 'Legal') as legal_rows,
    count_if(department_dept_name = 'Legal') as legal_events
from ranked
group by date_trunc('quarter', hire_date)
order by legal_events desc nulls first
