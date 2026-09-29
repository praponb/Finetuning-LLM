with metrics as (
    -- Ranking and gap features
    select
        id,
        channel,
        promo_code,
        discount_pct,
        sum(discount_pct) over (partition by promo_code, channel order by starts_on, id rows between unbounded preceding and current row) as running_discount_pct,
        row_number() over (order by discount_pct desc, id) as row_seq,
        rank() over (partition by channel order by discount_pct) as discount_pct_rank,
        avg(discount_pct) over (partition by promo_code, channel order by starts_on, id rows between 5 preceding and current row) as discount_pct_avg_5,
        ntile(3) over (partition by month(ends_on) order by discount_pct desc, id) as discount_pct_bucket
    from promotions
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from metrics
    qualify rank() over (partition by channel, promo_code order by discount_pct_bucket desc) = 1
),
metrics_promotions as (
    -- Running and moving aggregates
    select
        id,
        promo_code,
        discount_pct,
        discount_pct_rank,
        max(discount_pct_bucket) over (partition by promo_code order by discount_pct, id rows between unbounded preceding and current row) as running_max_discount_pct_bucket,
        sum(discount_pct_bucket) over (partition by promo_code order by discount_pct, id rows between unbounded preceding and current row) as running_sum_discount_pct_bucket,
        avg(discount_pct_bucket) over (partition by promo_code order by discount_pct, id rows between unbounded preceding and current row) as running_avg_discount_pct_bucket,
        sum(row_seq) over (partition by channel order by running_discount_pct, id rows between unbounded preceding and current row) as running_row_seq
    from latest
    where discount_pct between 0.13 and 0.19
),
cleaned as (
    -- Basic cleansing and defaults
    select *, case when discount_pct >= 0.35 then 'High' when discount_pct >= 0.12 then 'Medium' else 'Low' end as discount_pct_level
    from metrics_promotions
    where running_sum_discount_pct_bucket > 34
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from cleaned
    qualify rank() over (partition by promo_code order by running_sum_discount_pct_bucket nulls first) = 1
)
-- Multi-level aggregation
select
    case when running_row_seq >= 45 then 'LARGE' else 'SMALL' end as running_row_seq_tier,
    promo_code,
    round(avg(discount_pct_rank), 2) as avg_discount_pct_rank,
    round(avg(discount_pct), 2) as avg_discount_pct
from ranked
group by grouping sets ((case when running_row_seq >= 45 then 'LARGE' else 'SMALL' end, promo_code), (case when running_row_seq >= 45 then 'LARGE' else 'SMALL' end))
order by running_row_seq_tier nulls first;
