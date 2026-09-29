with deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by plan, status order by start_date nulls first, id) as latest_rank
        from subscriptions
    ) ranked
    where latest_rank = 1
),
remaining_keys as (
    -- Set intersection on keys
    select customer_id, id
    from deduped
    intersect all
    select customer_id, id
    from subscriptions
    where monthly_fee < 120
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped s
    join remaining_keys k on s.customer_id = k.customer_id and s.id = k.id
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        latest_rank,
        sum(monthly_fee) over w_grp as status_sum_monthly_fee,
        stddev_samp(monthly_fee) over (partition by customer_id) as customer_id_stddev_samp_monthly_fee
    from kept_rows
    window w_grp as (partition by status, plan)
),
with_dims as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.start_date,
        e.monthly_fee,
        b.age,
        b.city
    from metrics e
    left join customers b
        on e.customer_id = b.id
),
metrics_subscriptions as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        monthly_fee,
        city,
        min(age) over w_hist as start_date_month_min_age,
        rank() over w_hist_2 as monthly_fee_rank,
        round(age / sum(age) over (partition by customer_id), 4) as age_share,
        rank() over (partition by customer_id, city order by monthly_fee desc nulls first) as monthly_fee_rank_2,
        rank() over (partition by year(start_date), customer_id order by monthly_fee desc nulls last) as monthly_fee_rank_3
    from with_dims
    where monthly_fee < 90
    window w_hist as (partition by month(start_date), city), w_hist_2 as (partition by city order by monthly_fee desc nulls last)
)
-- Join a dimension and aggregate
select
    dim.city as customer_city,
    src.city as subscription_city,
    count(*) as num_rows,
    avg(src.age_share) as avg_age_share,
    sum(src.monthly_fee_rank_3) as total_monthly_fee_rank_3
from metrics_subscriptions src
join customers dim on src.customer_id = dim.id
group by dim.city, src.city
order by subscription_city nulls first;
