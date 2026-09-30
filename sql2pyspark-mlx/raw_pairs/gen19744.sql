with cleaned as (
    -- Keep recent, valid rows
    select
        *,
        initcap(trim(channel)) as channel_title,
        case when channel in ('mobile', 'web') then 'Core' else 'Other' end as channel_group
    from promotions
    where starts_on > date '2023-04-15'
),
scored as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (partition by date_trunc('month', ends_on), channel order by discount_pct desc) as discount_pct_cume_dist,
        avg(discount_pct) over (partition by month(starts_on), channel) as starts_on_month_avg_discount_pct,
        max(discount_pct) over (partition by month(starts_on), channel) as starts_on_month_max_discount_pct
    from cleaned
    where starts_on <= '2024-04-01'
),
scored_promotions as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (order by discount_pct_cume_dist desc, id) as row_seq,
        row_number() over (partition by channel, promo_code order by starts_on, id) as row_seq_2,
        sum(discount_pct) over (partition by channel order by starts_on_month_max_discount_pct, id rows between unbounded preceding and current row) as cumulative_discount_pct,
        row_number() over (partition by promo_code order by ends_on desc, id) as event_seq,
        row_number() over (partition by date_trunc('MONTH', ends_on), promo_code order by starts_on_month_avg_discount_pct desc, id) as row_seq_3
    from scored
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by channel order by ends_on desc, id) as latest_rank
    from scored_promotions
    qualify latest_rank = 1
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        channel_title,
        channel_group,
        discount_pct_cume_dist,
        row_seq_2,
        row_seq_3,
        latest_rank,
        ntile(3) over w_ordered as cumulative_discount_pct_bucket,
        ntile(10) over w_running as event_seq_bucket
    from deduped
    window w_ordered as (order by cumulative_discount_pct desc, id), w_running as (partition by promo_code order by event_seq desc, id)
)
-- Multi-level aggregation
select
    promo_code,
    case when row_seq_2 >= 6 then 'HIGH' else 'LOW' end as row_seq_2_tier,
    count(case when promo_code = 'SPRING10' then 1 end) as spring10_count,
    round(sum(discount_pct) / nullif(count(case when promo_code = 'VIP' then 1 end), 0), 2) as discount_pct_per_vip,
    count(*) filter (where channel = 'mobile') as mobile_rows,
    grouping(promo_code) as grouping_id
from metrics
group by grouping sets ((promo_code, case when row_seq_2 >= 6 then 'HIGH' else 'LOW' end), (promo_code), ())
having discount_pct_per_vip >= 0
