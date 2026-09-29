with with_windows as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (order by salary desc) as salary_cume_dist,
        lag(salary, 1) over (partition by job_title order by hire_date asc, id asc) as prev_salary,
        ntile(10) over (partition by dept_id order by salary desc, id asc) as salary_bucket
    from employees
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by manager_id, dept_id order by prev_salary asc, id asc) as rn
        from with_windows
    ) ranked
    where rn = 1
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by level order by salary asc, id asc) as dedup_rank
    from ranked
    qualify dedup_rank = 1
),
matched_keys as (
    -- Set difference on keys
    select manager_id
    from deduped
    except
    select manager_id
    from employees
    where manager_id is not null
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped s
    inner join matched_keys k on s.manager_id = k.manager_id
),
enriched as (
    -- Attach departments attributes
    select a.*, d.budget
    from kept_rows a
    left join departments d
        on a.dept_id = d.id
)
-- Join a dimension and aggregate
select
    d.location,
    a.job_title,
    count(*) as cnt,
    max(a.budget) as max_budget
from enriched a
left join departments d on a.dept_id = d.id
group by grouping sets ((d.location, a.job_title), (d.location), ())
