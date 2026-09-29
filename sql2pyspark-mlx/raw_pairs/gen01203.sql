with base as (
    -- Keep recent, valid rows
    select *, upper(dept_name) as dept_name_upper
    from departments
    where budget between 1274000 and 2613000
),
scored as (
    -- Ranking and gap features
    select
        id,
        location,
        budget,
        dept_name_upper,
        avg(budget) over (partition by dept_name order by budget, id rows between 3 preceding and 1 preceding) as budget_avg_3_prior,
        avg(budget) over (order by budget, id rows between 6 preceding and current row) as budget_avg_6
    from base
    where location in ('Tokyo', 'Chiang Mai', 'Berlin')
),
with_latest as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.location,
        s.dept_name_upper,
        s.budget_avg_6,
        top1.employee_job_title
    from scored s
    left join lateral (
            select c.job_title as employee_job_title
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) top1 on true
),
remaining_keys as (
    -- Set difference on keys
    select id, location
    from with_latest
    except all
    select id, location
    from departments
    where dept_name in ('Sales', 'Marketing', 'Legal')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_latest s
    join remaining_keys k on s.id = k.id and s.location = k.location
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        location,
        employee_job_title,
        dense_rank() over (order by budget_avg_6 desc nulls first) as budget_avg_6_dense_rank
    from verified
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        round(budget_avg_6_dense_rank, 2) as budget_avg_6_dense_rank_rounded,
        coalesce(employee_job_title, 'Unknown') as employee_job_title_filled
    from enriched
    where location is not null
      and employee_job_title is distinct from 'Intern'
),
enriched_departments as (
    -- Running and moving aggregates
    select
        budget_avg_6_dense_rank_rounded,
        employee_job_title_filled,
        sum(budget_avg_6_dense_rank_rounded) over (partition by employee_job_title) as employee_job_title_sum_budget_avg_6_dense_rank_rounded
    from valid
)
-- Multi-level aggregation
select
    case when employee_job_title_sum_budget_avg_6_dense_rank_rounded >= 376 then 'LARGE' else 'SMALL' end as employee_job_title_sum_budget_avg_6_dense_rank_rounded_tier,
    sum(employee_job_title_sum_budget_avg_6_dense_rank_rounded) as total_employee_job_title_sum_budget_avg_6_dense_rank_rounded,
    count(*) as num_rows
from enriched_departments
group by employee_job_title_sum_budget_avg_6_dense_rank_rounded_tier
having num_rows < 1000;
