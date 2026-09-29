with scored as (
    -- Running and moving aggregates
    select
        id,
        full_name,
        email,
        manager_id,
        is_remote,
        lead(salary) over w_part next_salary,
        lag(salary) over w_part prev_salary,
        row_number() over w_part row_seq
    from employees
    window w_part as (partition by manager_id order by hire_date, id)
),
enriched as (
    -- Attach employees attributes
    select
        src.id,
        src.email,
        src.manager_id,
        src.is_remote,
        src.row_seq,
        e.salary,
        e.level,
        e.full_name as employee_full_name
    from scored src
    left join employees e
        on src.manager_id = e.id
),
enriched_employees as (
    -- Attach employees attributes
    select src.*, p.salary as employee_salary
    from enriched src
    inner join employees p
        on src.manager_id = p.id
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        manager_id,
        row_seq,
        salary,
        level
    from (
        select *, rank() over (partition by manager_id, level order by employee_salary nulls last) as dedup_rank
        from enriched_employees
    ) ranked
    where dedup_rank = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        round(salary / sum(salary) over (partition by level), 4) salary_share,
        avg(salary) over (partition by manager_id order by salary, id rows between 5 preceding and current row) salary_avg_5
    from ranked
),
scored_employees as (
    -- Window metrics over several different partitions
    select
        id,
        salary,
        salary_avg_5,
        lead(salary_share) over w_hist next_salary_share,
        lag(salary_share) over w_hist prev_salary_share,
        lag(row_seq) over w_hist prev_row_seq,
        row_number() over w_hist event_seq,
        cume_dist() over (partition by level order by salary desc) salary_cume_dist
    from metrics
    window w_hist as (partition by manager_id order by salary, id)
)
-- Native SQL UNPIVOT (wide to long)
select
    id,
    metric,
    reading
from (
    select
        id,
        salary_cume_dist,
        next_salary_share,
        salary,
        prev_salary_share
    from scored_employees
) src
unpivot (
    reading for metric in (salary_cume_dist as SALARY_CUME_DIST, next_salary_share as NEXT_SALARY_SHARE_VAL, salary, prev_salary_share as PREV_SALARY_SHARE)
);
