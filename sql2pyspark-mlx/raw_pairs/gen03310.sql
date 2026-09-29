with base as (
    -- Basic cleansing and defaults
    select *, round(budget * 0.9, 2) budget_adjusted
    from departments
    where location is not null
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from base
    qualify row_number() over (partition by location order by budget_adjusted desc nulls last, id) = 1
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        lag(budget) over w_ordered prev_budget,
        lead(budget_adjusted) over w_ordered next_budget_adjusted,
        row_number() over w_ordered seq_no,
        rank() over (order by budget_adjusted desc nulls first) budget_adjusted_rank
    from deduped
    window w_ordered as (partition by location order by budget, id)
),
enriched_departments as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(budget) ignore nulls over (partition by dept_name order by budget, id rows between unbounded preceding and current row) last_known_budget,
        sum(budget) over (partition by dept_name order by next_budget_adjusted, id rows between unbounded preceding and current row) running_budget
    from enriched
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        round(budget, 2) budget_rounded,
        budget_adjusted_rank + running_budget budget_adjusted_rank_plus_running_budget,
        coalesce(location, 'OTHER') location_filled
    from enriched_departments
    where next_budget_adjusted between 36 and 58
),
stats as (
    -- Grouped metrics
    select
        seq_no,
        count(*) as num_rows,
        sum(case when dept_name = 'Finance' then next_budget_adjusted else 0 end) as finance_next_budget_adjusted
    from cleaned
    group by seq_no
    having sum(case when dept_name = 'Finance' then next_budget_adjusted else 0 end) >= 0
)
select seq_no, finance_next_budget_adjusted
from stats;
