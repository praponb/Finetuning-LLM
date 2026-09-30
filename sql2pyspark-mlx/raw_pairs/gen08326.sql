with filtered as (
    -- Keep recent, valid rows
    select
        *,
        substring(location, 1, 3) as location_prefix,
        round(budget * 1.15, 2) as adjusted_budget,
        coalesce(location, 'N/A') as resolved_location
    from departments
    where budget < 3261000
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        location,
        location_prefix,
        adjusted_budget,
        case when dept_name in ('Engineering', 'Sales') then 'Group A' else 'Other' end as dept_name_group,
        trim(dept_name) as clean_dept_name,
        coalesce(location, 'OTHER') as location_filled
    from filtered
    where location is not null
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        sum(adjusted_budget) over w_grp as last4_sum_adjusted_budget,
        max(adjusted_budget) over w_grp as last4_max_adjusted_budget,
        count(adjusted_budget) over w_grp as last4_count_adjusted_budget,
        avg(adjusted_budget) over w_grp as last4_avg_adjusted_budget
    from valid
    where adjusted_budget >= 53
    window w_grp as (partition by location order by adjusted_budget, id rows between 3 preceding and current row)
),
latest_match as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select s.*, latest.employee_hire_date
    from enriched s
    left join lateral (
            select c.hire_date as employee_hire_date
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) latest on true
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from latest_match s
    where not exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.hire_date >= s.employee_hire_date
        )
),
with_last as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.location,
        s.dept_name_group,
        s.location_filled,
        s.last4_sum_adjusted_budget,
        s.last4_count_adjusted_budget,
        s.last4_avg_adjusted_budget,
        x.job_title
    from kept s
    join lateral (
            select c.job_title
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    location,
    dept_name_group,
    location_filled,
    last4_count_adjusted_budget,
    last4_avg_adjusted_budget,
    job_title
from with_last
order by last4_avg_adjusted_budget desc;
