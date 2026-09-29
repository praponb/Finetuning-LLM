with valid as (
    -- Basic cleansing and defaults
    select
        id,
        email,
        country,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        age + credit_limit as age_plus_credit_limit,
        coalesce(city, 'UNKNOWN') as city_filled
    from customers
    where city is not null
      and city in ('Tokyo', 'Berlin')
),
with_latest as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.discount,
        latest.shipping_fee
    from valid s
    left join lateral (
            select c.discount, c.shipping_fee
            from orders c
            where c.customer_id = s.id
            order by c.shipping_fee desc, c.id desc
            limit 1
        ) latest on true
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        email,
        country,
        segment,
        signup_date,
        shipping_fee,
        avg(shipping_fee) over w_part as running_avg_shipping_fee,
        count(shipping_fee) over w_part as running_count_shipping_fee,
        sum(shipping_fee) over w_part as running_sum_shipping_fee
    from with_latest
    window w_part as (partition by segment, country order by age_plus_credit_limit, id rows between unbounded preceding and current row)
),
with_windows_customers as (
    -- Window metrics over several different partitions
    select
        id,
        country,
        segment,
        running_avg_shipping_fee,
        running_count_shipping_fee,
        min(running_avg_shipping_fee) over (partition by segment order by running_avg_shipping_fee, id rows between unbounded preceding and current row) as running_min_running_avg_shipping_fee,
        max(running_avg_shipping_fee) over (partition by segment order by running_avg_shipping_fee, id rows between unbounded preceding and current row) as running_max_running_avg_shipping_fee,
        avg(running_avg_shipping_fee) over (partition by segment order by running_avg_shipping_fee, id rows between unbounded preceding and current row) as running_avg_running_avg_shipping_fee
    from with_windows
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        running_count_shipping_fee,
        running_min_running_avg_shipping_fee,
        running_max_running_avg_shipping_fee,
        running_avg_running_avg_shipping_fee,
        ntile(5) over (order by running_count_shipping_fee, id) as running_count_shipping_fee_bucket,
        ntile(3) over (partition by country, segment order by running_count_shipping_fee, id) as running_count_shipping_fee_bucket_2
    from with_windows_customers
),
stats as (
    -- Multi-level aggregation
    select
        case when running_count_shipping_fee >= 13 then 'HIGH' else 'LOW' end as running_count_shipping_fee_tier,
        case when running_max_running_avg_shipping_fee >= 30 then 'PREMIUM' else 'STANDARD' end as running_max_running_avg_shipping_fee_tier,
        count(distinct id) as distinct_ids,
        round(avg(running_count_shipping_fee_bucket), 2) as avg_running_count_shipping_fee_bucket,
        percentile_approx(running_count_shipping_fee_bucket, array(0.25, 0.5, 0.75)) as running_count_shipping_fee_bucket_quartiles,
        round(avg(running_count_shipping_fee_bucket), 2) as avg_running_count_shipping_fee_bucket_2,
        approx_percentile(running_count_shipping_fee, array(0.25, 0.5, 0.75)) as running_count_shipping_fee_quartiles,
        grouping_id() as gid
    from metrics
    group by grouping sets ((case when running_count_shipping_fee >= 13 then 'HIGH' else 'LOW' end, case when running_max_running_avg_shipping_fee >= 30 then 'PREMIUM' else 'STANDARD' end), (case when running_count_shipping_fee >= 13 then 'HIGH' else 'LOW' end))
    having count(distinct id) > 1
)
select
    running_max_running_avg_shipping_fee_tier,
    distinct_ids,
    avg_running_count_shipping_fee_bucket,
    running_count_shipping_fee_bucket_quartiles,
    avg_running_count_shipping_fee_bucket_2,
    running_count_shipping_fee_quartiles,
    gid
from stats
order by gid desc nulls last
