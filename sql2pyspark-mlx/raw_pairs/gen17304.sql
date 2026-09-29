with metrics as (
    select
        *,
        max(cost) over w_recent as last3_max_cost,
        count(cost) over w_recent as last3_count_cost,
        min(cost) over w_recent as last3_min_cost,
        sum(cost) over w_recent as last3_sum_cost
    from shipments
    where delivered_date < '2024-11-01'
    window w_recent as (partition by order_id order by delivered_date, id rows between 2 preceding and current row)
),
metrics_shipments as (
    select
        *,
        dense_rank() over (partition by status order by last3_max_cost nulls last) as last3_max_cost_dense_rank,
        avg(cost) over (partition by status order by delivered_date, id rows between 3 preceding and current row) as cost_avg_3,
        last_value(last3_min_cost) ignore nulls over (partition by status order by last3_sum_cost, id rows between unbounded preceding and current row) as last_known_last3_min_cost,
        round(last3_max_cost / sum(last3_max_cost) over (partition by year(shipped_date), carrier), 4) as last3_max_cost_share,
        row_number() over (partition by status, order_id order by cost, id) as event_seq
    from metrics
),
latest as (
    select *, row_number() over (partition by order_id order by last3_min_cost desc, id) as dedup_rank
    from metrics_shipments
    qualify dedup_rank = 1
),
filtered as (
    select
        id,
        order_id,
        delivered_date,
        status,
        last3_max_cost,
        last3_count_cost,
        last3_min_cost,
        last3_max_cost_dense_rank,
        cost_avg_3,
        last3_max_cost_share,
        dedup_rank,
        cast(last3_max_cost_share as int) as last3_max_cost_share_int,
        coalesce(carrier, 'DIRECT') as resolved_carrier
    from latest
    where delivered_date >= to_date('2023-07-11')
      and carrier is not null
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by order_id, status order by dedup_rank, id) as dedup_rank_2
        from filtered
    ) x
    where dedup_rank_2 = 1
)
select
    case when cost_avg_3 >= 91 then 'PREMIUM' else 'STANDARD' end as cost_avg_3_tier,
    date_trunc('week', delivered_date) as week_start,
    round(sum(last3_max_cost) / nullif(count(case when status = 'DELIVERED' then 1 end), 0), 2) as last3_max_cost_per_delivered,
    avg(last3_max_cost_share_int) filter (where status = 'LOST') as lost_avg_last3_max_cost_share_int,
    size(collect_set(status)) as num_status,
    percent_rank() over (partition by case when cost_avg_3 >= 91 then 'PREMIUM' else 'STANDARD' end order by size(collect_set(status)) desc) as num_status_percent_rank
from deduped
group by case when cost_avg_3 >= 91 then 'PREMIUM' else 'STANDARD' end, date_trunc('week', delivered_date);
