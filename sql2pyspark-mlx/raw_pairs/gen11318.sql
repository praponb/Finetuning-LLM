with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(budget * 1.1, 2) as budget_scaled,
        coalesce(dept_name, 'UNKNOWN') as dept_name_clean
    from departments
    where budget between 2581000 and 3766000
),
base as (
    -- Keep recent, valid rows
    select
        id,
        dept_name,
        budget,
        budget_scaled,
        dept_name_clean,
        greatest(budget_scaled, budget) as greatest_budget_scaled_budget
    from filtered
    where dept_name is distinct from 'Finance'
      and budget between 2270000 and 2550000
),
matched_keys as (
    -- Set difference on keys
    select id
    from base
    except
    select id
    from departments
    where dept_name in ('Engineering', 'Sales', 'Legal')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    join matched_keys k on s.id = k.id
),
metrics as (
    -- Running and moving aggregates
    select
        dept_name,
        budget,
        greatest_budget_scaled_budget,
        round(budget / sum(budget) over (partition by dept_name), 4) as budget_share,
        dense_rank() over w_part as greatest_budget_scaled_budget_dense_rank,
        sum(greatest_budget_scaled_budget) over w_ordered as dept_name_sum_greatest_budget_scaled_budget,
        rank() over (partition by dept_name order by budget_scaled desc nulls first) as budget_scaled_rank,
        min(budget) over w_ordered as dept_name_min_budget
    from reconciled
    where budget >= 1888000
    window w_part as (order by greatest_budget_scaled_budget nulls last), w_ordered as (partition by dept_name)
),
metrics_departments as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by dept_name order by greatest_budget_scaled_budget_dense_rank desc nulls first) as greatest_budget_scaled_budget_dense_rank_percent_rank,
        percent_rank() over (partition by dept_name order by budget nulls first) as budget_percent_rank,
        stddev_samp(dept_name_sum_greatest_budget_scaled_budget) over (partition by dept_name) as dept_name_stddev_samp_dept_name_sum_greatest_budget_scaled_budget
    from metrics
),
filtered_departments as (
    -- Keep recent, valid rows
    select
        *,
        coalesce(budget_share, 0) as budget_share_filled,
        coalesce(dept_name, 'DIRECT') as dept_name_clean
    from metrics_departments
    where greatest_budget_scaled_budget_dense_rank_percent_rank between 0.55 and 0.61
),
enriched as (
    -- Window metrics over several different partitions
    select
        budget,
        greatest_budget_scaled_budget_dense_rank,
        dept_name_sum_greatest_budget_scaled_budget,
        greatest_budget_scaled_budget_dense_rank_percent_rank,
        dept_name_stddev_samp_dept_name_sum_greatest_budget_scaled_budget,
        budget_share_filled,
        dept_name_clean,
        dense_rank() over (order by greatest_budget_scaled_budget) as greatest_budget_scaled_budget_dense_rank_2
    from filtered_departments
)
select
    budget,
    greatest_budget_scaled_budget_dense_rank_percent_rank,
    dept_name_stddev_samp_dept_name_sum_greatest_budget_scaled_budget,
    dept_name_clean,
    greatest_budget_scaled_budget_dense_rank_2
from enriched;
