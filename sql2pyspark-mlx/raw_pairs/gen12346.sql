with cleaned as (
    select
        *,
        trim(name) clean_name,
        coalesce(segment, 'N/A') segment_filled
    from customers
    where city is not null
),
ranked as (
    select
        id,
        name,
        email,
        country,
        segment,
        is_active,
        credit_limit,
        age,
        clean_name,
        segment_filled
    from cleaned
    qualify row_number() over (partition by segment order by credit_limit desc, id) = 1
),
metrics as (
    select
        id,
        name,
        is_active,
        credit_limit,
        segment_filled,
        max(credit_limit) over w_grp running_max_credit_limit,
        count(credit_limit) over w_grp running_count_credit_limit,
        min(credit_limit) over w_grp running_min_credit_limit,
        avg(credit_limit) over w_grp running_avg_credit_limit,
        stddev_samp(credit_limit) over (partition by segment) segment_stddev_samp_credit_limit
    from ranked
    window w_grp as (partition by country, segment order by age, id rows between unbounded preceding and current row)
),
not_flagged as (
    select s.*
    from metrics s
    where not exists (
            select 1
            from orders x
            where x.customer_id = s.id
              and x.shipping_fee > 12
        )
),
stats as (
    select
        case when segment_stddev_samp_credit_limit >= 17648 then 'LARGE' else 'SMALL' end as segment_stddev_samp_credit_limit_tier,
        case when running_count_credit_limit >= 18 then 'HIGH' else 'LOW' end as running_count_credit_limit_tier,
        percentile_approx(segment_stddev_samp_credit_limit, array(0.25, 0.5, 0.75)) as segment_stddev_samp_credit_limit_quartiles,
        round(avg(running_avg_credit_limit), 2) as avg_running_avg_credit_limit,
        count(*) as num_rows,
        approx_percentile(credit_limit, array(0.25, 0.5, 0.75)) as credit_limit_quartiles
    from not_flagged
    group by segment_stddev_samp_credit_limit_tier, running_count_credit_limit_tier
)
select
    segment_stddev_samp_credit_limit_tier,
    running_count_credit_limit_tier,
    avg_running_avg_credit_limit
from stats;
