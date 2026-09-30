with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        to_date(paid_at) as paid_at_date,
        case when amount >= 1530 then 'High' when amount >= 460 then 'Medium' else 'Low' end as amount_band,
        coalesce(method, 'DIRECT') as resolved_method
    from payments
    where paid_at > timestamp '2023-09-13 21:00:00'
      and method is not null
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by method order by paid_at, id) > interval 7 days then 1 else 0 end as is_new_burst
    from cleaned
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as island_id
    from flagged
)
-- Multi-level aggregation
select
    date_trunc('year', paid_at_date) as year_start,
    case when island_id >= 8 then 'LARGE' else 'SMALL' end as island_id_tier,
    count(*) as num_rows,
    sum(case when method = 'bank_transfer' then is_new_burst else 0 end) as bank_transfer_is_new_burst,
    count(distinct status) as distinct_status,
    max(paid_at) as last_seen,
    count(*) as row_count,
    round(avg(is_new_burst), 2) as avg_is_new_burst,
    dense_rank() over (partition by date_trunc('year', paid_at_date) order by count(*) desc) as row_count_dense_rank
from islands
group by date_trunc('year', paid_at_date), case when island_id >= 8 then 'LARGE' else 'SMALL' end
order by year_start
