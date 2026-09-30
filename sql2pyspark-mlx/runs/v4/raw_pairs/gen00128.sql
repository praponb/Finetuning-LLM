with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        email,
        dept_id,
        manager_id,
        hire_date,
        is_remote,
        level,
        case when salary >= 159000 then 'High' when salary >= 63000 then 'Medium' else 'Low' end as salary_band
    from employees
    where job_title is not null
),
latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by level order by hire_date desc nulls last, id) as row_num
    from filtered
    qualify row_num = 1
),
with_dims as (
    -- Attach departments attributes
    select a.*, d.location
    from latest a
    inner join departments d
        on a.dept_id = d.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (order by row_num, id) as row_seq,
        first_value(level, true) over (partition by level order by row_num, id rows between unbounded preceding and current row) as first_known_level
    from with_dims
)
-- Aggregate with conditional metrics
select
    date_trunc('quarter', hire_date) as quarter_start,
    approx_percentile(row_seq, array(0.25, 0.5, 0.75)) as row_seq_quartiles,
    sum(row_seq) filter (where location = 'Singapore') as singapore_total_row_seq,
    grouping_id() as grouping_level
from with_windows
group by rollup (date_trunc('quarter', hire_date))
