with base as (
    -- Filter the raw rows and derive helper columns
    select *, case when cost >= 99 then 'High' when cost >= 32 then 'Medium' else 'Low' end as cost_tier
    from shipments
    where carrier is distinct from 'FedEx'
      and order_id is not null
),
scored as (
    -- Ranking and gap features
    select
        id,
        order_id,
        shipped_date,
        delivered_date,
        percent_rank() over (partition by carrier order by cost nulls last) as cost_percent_rank,
        sum(cost) over (partition by order_id order by delivered_date, id rows between unbounded preceding and current row) as running_cost,
        avg(cost) over (partition by order_id) as order_id_avg_cost
    from base
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        shipped_date,
        delivered_date,
        cost_percent_rank,
        running_cost,
        sum(running_cost) over (partition by order_id order by shipped_date, id rows between unbounded preceding and current row) as running_running_cost,
        avg(running_cost) over (partition by order_id order by delivered_date, id rows between 3 preceding and current row) as running_cost_avg_3,
        sum(cost_percent_rank) over w_hist as cumulative_cost_percent_rank
    from scored
    window w_hist as (partition by order_id order by delivered_date, id rows between unbounded preceding and current row)
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(10) over (order by running_cost_avg_3 desc, id) as running_cost_avg_3_bucket,
        avg(cost_percent_rank) over w_recent as cost_percent_rank_avg_5,
        lead(cost_percent_rank, 1, 0) over w_ordered as next_cost_percent_rank,
        min(cumulative_cost_percent_rank) over (partition by date_trunc('week', delivered_date)) as delivered_date_period_min_cumulative_cost_percent_rank,
        row_number() over w_ordered as row_seq
    from with_windows
    where delivered_date <= '2023-08-01'
    window w_recent as (order by delivered_date, id rows between 5 preceding and current row), w_ordered as (order by cumulative_cost_percent_rank, id)
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        shipped_date,
        delivered_date,
        running_cost,
        running_running_cost,
        running_cost_avg_3,
        cumulative_cost_percent_rank,
        running_cost_avg_3_bucket,
        cost_percent_rank_avg_5,
        delivered_date_period_min_cumulative_cost_percent_rank,
        row_seq,
        round(running_cost, 1) as running_cost_rounded,
        least(row_seq, next_cost_percent_rank) as least_row_seq_next_cost_percent_rank
    from metrics
    where delivered_date > date '2023-09-21'
)
select
    id,
    delivered_date,
    running_cost_avg_3,
    running_cost_avg_3_bucket,
    cost_percent_rank_avg_5,
    delivered_date_period_min_cumulative_cost_percent_rank,
    running_cost_rounded,
    least_row_seq_next_cost_percent_rank
from valid
order by cost_percent_rank_avg_5 desc, delivered_date desc
