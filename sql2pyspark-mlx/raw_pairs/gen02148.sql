with valid as (
    -- Keep recent, valid rows
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        trim(promo_code) as clean_promo_code,
        coalesce(channel, 'OTHER') as resolved_channel
    from promotions
    where ends_on > date '2023-11-12'
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        ends_on,
        discount_pct,
        clean_promo_code,
        ntile(4) over (partition by channel, promo_code order by discount_pct desc, id) as discount_pct_bucket,
        round(discount_pct / sum(discount_pct) over (partition by promo_code, channel), 4) as discount_pct_share,
        row_number() over (partition by promo_code, channel order by starts_on, id) as event_seq
    from valid
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        ends_on,
        discount_pct_share,
        event_seq,
        avg(discount_pct_share) over w_running as running_avg_discount_pct_share,
        min(discount_pct_share) over w_running as running_min_discount_pct_share,
        max(discount_pct_share) over w_running as running_max_discount_pct_share,
        ntile(4) over (order by discount_pct_share desc, id) as discount_pct_share_bucket
    from enriched
    window w_running as (order by discount_pct, id rows between unbounded preceding and current row)
),
cleaned as (
    -- Basic cleansing and defaults
    select *, least(discount_pct_share, running_min_discount_pct_share) as least_discount_pct_share_running_min_discount_pct_share
    from metrics
    where discount_pct_share >= 0.56
),
cleaned_promotions as (
    -- Filter the raw rows and derive helper columns
    select *, datediff(current_date(), ends_on) / 365 as years_since_ends_on
    from cleaned
    where event_seq < 7
),
grouped as (
    -- Aggregate with conditional metrics
    select
        case when discount_pct_share >= 0.82 then 'HIGH' else 'LOW' end as discount_pct_share_tier,
        date_trunc('quarter', ends_on) as quarter_start,
        percentile_approx(years_since_ends_on, 0.95) as years_since_ends_on_p95,
        count(distinct id) as distinct_ids,
        count(distinct id) as distinct_ids_2,
        count(*) as cnt,
        avg(years_since_ends_on) as avg_years_since_ends_on
    from cleaned_promotions
    group by rollup (case when discount_pct_share >= 0.82 then 'HIGH' else 'LOW' end, date_trunc('quarter', ends_on))
    having count(distinct id) > 1
)
select
    years_since_ends_on_p95,
    distinct_ids_2,
    cnt,
    avg_years_since_ends_on
from grouped
order by distinct_ids_2 desc, cnt
