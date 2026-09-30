with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        datediff(shipped_date, delivered_date) / 7 as weeks_delivered_date_to_shipped_date,
        case when status = 'IN_TRANSIT' then 1 else 0 end as is_in_transit
    from shipments
    where delivered_date >= date '2023-05-08'
      and carrier is not distinct from 'FedEx'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(cost, true) over (partition by year(delivered_date), status order by shipped_date, id rows between unbounded preceding and current row) as first_known_cost,
        ntile(5) over (partition by order_id order by weeks_delivered_date_to_shipped_date desc, id) as weeks_delivered_date_to_shipped_date_bucket
    from filtered
),
with_windows_shipments as (
    -- Running and moving aggregates
    select
        id,
        carrier,
        delivered_date,
        weeks_delivered_date_to_shipped_date,
        lead(first_known_cost, 1) over (partition by year(shipped_date), carrier order by first_known_cost, id) as next_first_known_cost,
        round(first_known_cost / sum(first_known_cost) over (partition by order_id), 4) as first_known_cost_share,
        first_value(weeks_delivered_date_to_shipped_date) ignore nulls over (partition by date_trunc('WEEK', shipped_date), status order by shipped_date, id rows between unbounded preceding and current row) as first_known_weeks_delivered_date_to_shipped_date,
        avg(is_in_transit) over (partition by month(delivered_date), carrier order by weeks_delivered_date_to_shipped_date, id rows between 6 preceding and 1 preceding) as is_in_transit_avg_6_prior
    from with_windows
    where status = 'DELIVERED'
),
deduped as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by carrier order by first_known_cost_share desc nulls first) as row_num
    from with_windows_shipments
    qualify row_num = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        delivered_date,
        next_first_known_cost,
        first_known_cost_share,
        first_known_weeks_delivered_date_to_shipped_date,
        row_num,
        lag(weeks_delivered_date_to_shipped_date) over (partition by carrier order by row_num, id) as prev_weeks_delivered_date_to_shipped_date,
        lag(is_in_transit_avg_6_prior) over (partition by carrier order by row_num, id) as prev_is_in_transit_avg_6_prior,
        lead(is_in_transit_avg_6_prior) over (partition by carrier order by row_num, id) as next_is_in_transit_avg_6_prior,
        row_number() over (partition by carrier order by row_num, id) as row_seq,
        sum(next_first_known_cost) over (partition by carrier order by first_known_weeks_delivered_date_to_shipped_date, id rows between unbounded preceding and current row) as cumulative_next_first_known_cost
    from deduped
)
-- Multi-level aggregation
select
    case when prev_weeks_delivered_date_to_shipped_date >= 88 then 'LARGE' else 'SMALL' end as prev_weeks_delivered_date_to_shipped_date_tier,
    case when row_seq >= 7 then 'PREMIUM' else 'STANDARD' end as row_seq_tier,
    percentile_approx(first_known_cost_share, 0.9) as first_known_cost_share_p90,
    sum(cumulative_next_first_known_cost) as total_cumulative_next_first_known_cost,
    count(*) as event_count
from enriched
group by case when prev_weeks_delivered_date_to_shipped_date >= 88 then 'LARGE' else 'SMALL' end, case when row_seq >= 7 then 'PREMIUM' else 'STANDARD' end
order by event_count;
