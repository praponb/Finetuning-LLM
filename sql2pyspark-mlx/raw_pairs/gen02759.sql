with base as (
    -- Basic cleansing and defaults
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        trim(promo_code) as clean_promo_code,
        round(discount_pct * 0.85, 2) as discount_pct_scaled
    from promotions
    where channel is not null
),
scored as (
    -- Ranking and gap features
    select
        id,
        channel,
        promo_code,
        row_number() over (partition by channel order by discount_pct_scaled, id) as row_seq,
        ntile(10) over (order by discount_pct_scaled desc, id) as discount_pct_scaled_bucket,
        avg(discount_pct_scaled) over (partition by month(ends_on) order by starts_on, id rows between 2 preceding and current row) as discount_pct_scaled_avg_2,
        sum(discount_pct_scaled) over (partition by year(ends_on) order by starts_on, id rows between unbounded preceding and current row) as running_discount_pct_scaled,
        sum(discount_pct_scaled) over (partition by channel, promo_code order by discount_pct_scaled, id rows between unbounded preceding and current row) as cumulative_discount_pct_scaled
    from base
    where channel = 'partner'
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (partition by promo_code order by running_discount_pct_scaled) as running_discount_pct_scaled_cume_dist,
        sum(discount_pct_scaled_avg_2) over (partition by promo_code order by cumulative_discount_pct_scaled, id rows between unbounded preceding and current row) as cumulative_discount_pct_scaled_avg_2,
        sum(running_discount_pct_scaled) over (order by running_discount_pct_scaled, id rows between unbounded preceding and current row) as running_running_discount_pct_scaled
    from scored
),
scored_promotions as (
    -- Window metrics over several different partitions
    select
        id,
        channel,
        promo_code,
        row_seq,
        cumulative_discount_pct_scaled_avg_2,
        row_number() over (partition by channel, promo_code order by discount_pct_scaled_avg_2, id) as row_seq_2,
        max(discount_pct_scaled_avg_2) over (partition by channel) as channel_max_discount_pct_scaled_avg_2
    from metrics
)
-- Multi-level aggregation
select
    case when channel_max_discount_pct_scaled_avg_2 >= 73 then 'LARGE' else 'SMALL' end as channel_max_discount_pct_scaled_avg_2_tier,
    avg(row_seq_2) filter (where promo_code = 'BUNDLE') as bundle_avg_row_seq_2,
    count(*) as event_count,
    count(*) filter (where channel = 'partner') as partner_rows
from scored_promotions
group by rollup (case when channel_max_discount_pct_scaled_avg_2 >= 73 then 'LARGE' else 'SMALL' end)
order by partner_rows nulls last;
