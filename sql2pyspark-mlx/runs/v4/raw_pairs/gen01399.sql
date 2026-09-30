with ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from subscriptions
    qualify rank() over (partition by customer_id order by end_date nulls first) = 1
),
enriched as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        ntile(4) over (partition by status order by monthly_fee desc, id) monthly_fee_bucket,
        lead(monthly_fee, 1) over (partition by status order by monthly_fee, id) next_monthly_fee,
        lead(monthly_fee, 1) over (order by end_date, id) next_monthly_fee_2,
        ntile(10) over w_hist monthly_fee_bucket_2
    from ranked
    window w_hist as (partition by customer_id, status order by monthly_fee, id)
),
scored as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_bucket,
        next_monthly_fee,
        next_monthly_fee_2,
        monthly_fee_bucket_2,
        avg(monthly_fee) over (partition by status order by end_date, id rows between 5 preceding and current row) monthly_fee_avg_5,
        ntile(4) over w_recent next_monthly_fee_2_bucket,
        dense_rank() over (order by next_monthly_fee nulls first) next_monthly_fee_dense_rank
    from enriched
    window w_recent as (partition by plan order by next_monthly_fee_2 desc, id)
),
matched_keys as (
    -- Set intersection on keys
    select status
    from scored
    intersect all
    select status
    from subscriptions
    where customer_id is null
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join matched_keys k on s.status = k.status
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by status order by monthly_fee desc nulls first) monthly_fee_percent_rank,
        percent_rank() over (partition by customer_id order by monthly_fee_bucket_2 desc nulls first) monthly_fee_bucket_2_percent_rank,
        round(monthly_fee_bucket / sum(monthly_fee_bucket) over (partition by plan), 4) monthly_fee_bucket_share
    from kept_rows
),
filtered as (
    -- Keep recent, valid rows
    select
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_bucket,
        next_monthly_fee,
        next_monthly_fee_2,
        monthly_fee_avg_5,
        monthly_fee_percent_rank,
        monthly_fee_bucket_share,
        add_months(start_date, 6) start_date_plus_months,
        least(monthly_fee_bucket_2, monthly_fee) least_monthly_fee_bucket_2_monthly_fee
    from with_windows
    where start_date >= date '2023-04-05'
      and customer_id is not null
      and month(end_date) in (4, 5, 11)
),
grouped as (
    -- Grouped metrics
    select
        status,
        sum(case when plan = 'premium' then next_monthly_fee else 0 end) as premium_next_monthly_fee,
        sum(case when plan = 'premium' then monthly_fee_percent_rank else 0 end) as premium_monthly_fee_percent_rank,
        count(case when status = 'CANCELLED' then 1 end) as cancelled_count
    from filtered
    group by status
)
select
    status,
    premium_next_monthly_fee,
    cancelled_count,
    case when status rlike '^(CANCELLED|ACTIVE)$' then 'GROUP_B' when status rlike '^(PAUSED|EXPIRED)$' then 'CORE' else 'GROUP_A' end as status_zone
from grouped
order by cancelled_count desc nulls first
