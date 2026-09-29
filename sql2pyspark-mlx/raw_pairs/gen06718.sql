with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, concat(plan, ' ', status) as plan_status
    from subscriptions
    where customer_id is not null
      and start_date >= date '2023-09-10'
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by plan order by start_date desc nulls first, id) as rank_in_group
    from filtered
    qualify rank_in_group = 1
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        rank_in_group,
        stddev_samp(monthly_fee) over (partition by year(start_date), customer_id) as start_date_year_stddev_samp_monthly_fee,
        avg(monthly_fee) over (partition by plan order by monthly_fee, id rows between 6 preceding and 1 preceding) as monthly_fee_avg_6_prior,
        last_value(monthly_fee, true) over (partition by plan order by rank_in_group, id rows between unbounded preceding and current row) as last_known_monthly_fee,
        round(monthly_fee / sum(monthly_fee) over (partition by status), 4) as monthly_fee_share
    from deduped
),
metrics_subscriptions as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        end_date,
        rank_in_group,
        lag(monthly_fee_share, 2) over w_running as prev_monthly_fee_share,
        lead(start_date_year_stddev_samp_monthly_fee, 2) over (partition by customer_id order by start_date_year_stddev_samp_monthly_fee, id) as next_start_date_year_stddev_samp_monthly_fee,
        avg(start_date_year_stddev_samp_monthly_fee) over (partition by customer_id order by last_known_monthly_fee, id rows between 3 preceding and current row) as start_date_year_stddev_samp_monthly_fee_avg_3
    from metrics
    window w_running as (order by monthly_fee_avg_6_prior, id)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from metrics_subscriptions
    qualify row_number() over (partition by customer_id order by rank_in_group, id) = 1
),
remaining_keys as (
    -- Set intersection on keys
    select customer_id, id
    from latest
    intersect all
    select customer_id, id
    from subscriptions
    where status = 'PAUSED'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    join remaining_keys k on s.customer_id = k.customer_id and s.id = k.id
)
-- Aggregate with conditional metrics
select
    customer_id,
    count(distinct id) as distinct_ids,
    count(*) as num_rows,
    grouping(customer_id) as grouping_level
from reconciled
group by cube (customer_id)
having num_rows > 1
order by distinct_ids nulls first
