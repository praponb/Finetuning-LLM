with with_windows as (
    -- Ranking and gap features
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        ntile(3) over (partition by channel order by discount_pct desc, id) as discount_pct_bucket,
        percent_rank() over (partition by channel order by discount_pct) as discount_pct_percent_rank
    from promotions
),
remaining_keys as (
    -- Set intersection on keys
    select channel
    from with_windows
    intersect all
    select channel
    from promotions
    where discount_pct between 0.2 and 0.22
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    join remaining_keys k on s.channel = k.channel
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct_bucket,
        discount_pct_percent_rank,
        floor(discount_pct_percent_rank) as discount_pct_percent_rank_whole
    from kept_rows
    where starts_on > date '2024-02-29'
      and channel is not null
      and discount_pct < 0.32
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct_bucket,
        discount_pct_percent_rank,
        greatest(discount_pct_percent_rank, discount_pct_percent_rank_whole) as greatest_discount_pct_percent_rank_discount_pct_percent_rank_whole
    from cleaned
    where channel is not null
      and discount_pct_percent_rank >= 0.4
),
with_window as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct
    from filtered s
    left join promotions pr
        on s.channel = pr.channel
       and s.starts_on >= pr.starts_on
       and s.starts_on < pr.ends_on + interval '30' day
)
-- Multi-level aggregation
select
    promo_id,
    date_trunc('year', starts_on) as year_start,
    date_trunc('month', ends_on) as activity_month,
    max(greatest_discount_pct_percent_rank_discount_pct_percent_rank_whole) as max_greatest_discount_pct_percent_rank_discount_pct_percent_rank_whole,
    round(avg(promo_id), 2) as avg_promo_id,
    count(*) as row_count
from with_window
group by cube (promo_id, date_trunc('year', starts_on), date_trunc('month', ends_on))
order by promo_id;
