with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        full_name,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        lower(email) as email_lower
    from employees
    where job_title is null
),
enriched as (
    -- Attach employees attributes
    select
        e.*,
        d.level as employee_level,
        d.full_name as employee_full_name
    from filtered e
    join employees d
        on e.manager_id = d.id
),
enriched_employees as (
    -- Ranking and gap features
    select
        id,
        full_name,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        email_lower,
        employee_level,
        employee_full_name,
        lead(salary, 1, 0) over (partition by job_title order by salary, id) as next_salary,
        last_value(employee_level) ignore nulls over (partition by level, employee_level order by salary, id rows between unbounded preceding and current row) as last_known_employee_level
    from enriched
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by level, manager_id order by next_salary desc nulls last, id) as rank_in_group
        from enriched_employees
    ) ranked
    where rank_in_group = 1
)
-- Running and moving aggregates
select
    *,
    lead(salary, 1) over (partition by manager_id order by rank_in_group, id) as next_salary_2,
    ntile(10) over w_ordered as salary_bucket,
    avg(salary) over (order by rank_in_group, id rows between 6 preceding and current row) as salary_avg_6
from ranked
window w_ordered as (partition by level order by salary desc, id)
