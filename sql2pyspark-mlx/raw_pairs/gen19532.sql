with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when dept_name in ('Engineering', 'Finance') then 'Tier 1' else 'Other' end as dept_name_group,
        coalesce(location, 'N/A') as location_filled
    from departments
    where dept_name is not null
      and location is distinct from 'Chiang Mai'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        dept_name,
        location,
        budget,
        avg(budget) over (partition by dept_name order by budget, id rows between 3 preceding and current row) as budget_avg_3,
        max(budget) over (partition by location) as location_max_budget,
        ntile(5) over (partition by location order by budget, id) as budget_bucket,
        ntile(5) over w_ordered as budget_bucket_2
    from filtered
    where location in ('London', 'Bangkok', 'Tokyo')
    window w_ordered as (partition by dept_name order by budget, id)
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows s
    where not exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.job_title not in ('Intern', 'Engineer', 'Manager')
        )
),
with_latest as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select s.*, x.salary
    from kept s
    join lateral (
            select c.salary
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) x on true
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        location,
        budget_avg_3,
        location_max_budget,
        salary,
        sum(location_max_budget) over (partition by location order by budget_bucket_2, id rows between unbounded preceding and current row) as running_location_max_budget,
        avg(budget_bucket_2) over (partition by location order by budget_avg_3, id rows between 2 preceding and current row) as budget_bucket_2_avg_2,
        last_value(budget_avg_3, true) over w_grp as last_known_budget_avg_3,
        sum(location_max_budget) over (partition by location order by budget_avg_3, id rows between unbounded preceding and current row) as running_location_max_budget_2
    from with_latest
    window w_grp as (partition by dept_name order by budget_avg_3, id rows between unbounded preceding and current row)
)
-- Ranking and gap features
select
    id,
    location,
    budget_avg_3,
    location_max_budget,
    running_location_max_budget,
    last_known_budget_avg_3,
    running_location_max_budget_2,
    min(salary) over w_running as last7_min_salary,
    max(salary) over w_running as last7_max_salary,
    sum(salary) over w_running as last7_sum_salary,
    avg(salary) over w_running as last7_avg_salary
from enriched
window w_running as (partition by location order by location_max_budget, id rows between 6 preceding and current row);
