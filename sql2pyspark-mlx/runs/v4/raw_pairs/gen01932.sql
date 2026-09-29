with cleaned as (
    -- Basic cleansing and defaults
    select *, concat(promo_code, ', ', channel) as promo_code_channel
    from promotions
    where starts_on >= date '2023-07-17'
      and ends_on > '2023-05-01'
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_code_channel,
        sum(discount_pct) over w_ordered as running_discount_pct,
        percent_rank() over (partition by promo_code, channel order by discount_pct) as discount_pct_percent_rank,
        round(discount_pct / sum(discount_pct) over (partition by channel, promo_code), 4) as discount_pct_share
    from cleaned
    window w_ordered as (order by starts_on, id rows between unbounded preceding and current row)
),
remaining_keys as (
    -- Set difference on keys
    select channel, promo_code
    from with_windows
    except
    select channel, promo_code
    from promotions
    where promo_code in ('BUNDLE', 'WELCOME5', 'SPRING10')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join remaining_keys k on s.channel = k.channel and s.promo_code = k.promo_code
),
base as (
    -- Basic cleansing and defaults
    select
        channel,
        starts_on,
        ends_on,
        discount_pct,
        running_discount_pct,
        discount_pct_percent_rank,
        discount_pct_share,
        floor(running_discount_pct) as running_discount_pct_whole
    from kept_rows
    where ends_on >= date '2023-10-12'
      and channel is not null
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        date_trunc('WEEK', starts_on) as starts_on_period,
        substring(channel, 1, 3) as channel_prefix
    from base
    where ends_on >= date '2024-04-23'
),
metrics as (
    -- Ranking and gap features
    select
        *,
        round(running_discount_pct / sum(running_discount_pct) over (partition by channel), 4) as running_discount_pct_share,
        cume_dist() over w_recent as running_discount_pct_whole_cume_dist,
        max(running_discount_pct_whole) over (partition by channel) as channel_max_running_discount_pct_whole
    from valid
    window w_recent as (order by running_discount_pct_whole desc nulls first)
),
with_window as (
    -- Non-equi join on a validity window
    select
        src.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct as promotion_discount_pct
    from metrics src
    inner join promotions pr
        on coalesce(src.channel, 'store') = pr.channel
       and src.ends_on >= pr.starts_on
       and src.ends_on <= date_add(pr.ends_on, 10)
)
select
    channel,
    starts_on,
    ends_on,
    running_discount_pct,
    discount_pct_percent_rank,
    running_discount_pct_whole,
    channel_prefix,
    running_discount_pct_whole_cume_dist,
    channel_max_running_discount_pct_whole
from with_window
order by discount_pct_percent_rank desc nulls first;
