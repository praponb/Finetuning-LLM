with filtered as (
    select
        *,
        length(channel) channel_length,
        concat_ws(',', promo_code, channel) promo_code_channel_key,
        coalesce(channel, 'UNKNOWN') resolved_channel
    from promotions
    where channel is not null
      and channel != 'store'
),
with_windows as (
    select
        *,
        avg(channel_length) over (partition by channel order by starts_on, id rows between 3 preceding and 1 preceding) channel_length_avg_3_prior,
        ntile(3) over (partition by date_trunc('MONTH', starts_on), promo_code order by channel_length desc, id) channel_length_bucket
    from filtered
),
latest as (
    select
        id,
        ends_on,
        discount_pct,
        channel_length,
        resolved_channel,
        channel_length_avg_3_prior,
        channel_length_bucket
    from with_windows
    qualify row_number() over (partition by channel, promo_code order by channel_length desc, id) = 1
),
valid as (
    select *, channel_length_bucket * discount_pct channel_length_bucket_times_discount_pct
    from latest
    where ends_on > date '2023-10-12'
),
with_windows_promotions as (
    select
        *,
        count(channel_length_avg_3_prior) over w_ordered running_count_channel_length_avg_3_prior,
        max(channel_length_avg_3_prior) over w_ordered running_max_channel_length_avg_3_prior,
        avg(channel_length_avg_3_prior) over w_ordered running_avg_channel_length_avg_3_prior
    from valid
    window w_ordered as (order by channel_length_avg_3_prior, id rows between unbounded preceding and current row)
)
select
    case when channel_length_bucket >= 7 then 'PREMIUM' else 'STANDARD' end as channel_length_bucket_tier,
    date_trunc('year', ends_on) as year_start,
    case when running_avg_channel_length_avg_3_prior >= 88 then 'HIGH' else 'LOW' end as running_avg_channel_length_avg_3_prior_tier,
    count(*) as num_rows,
    count(distinct id) as distinct_ids,
    percentile_approx(channel_length_bucket, 0.95) as channel_length_bucket_p95
from with_windows_promotions
group by 1, 2, 3
order by channel_length_bucket_p95 nulls last
