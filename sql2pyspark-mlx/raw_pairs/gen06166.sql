with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        day(starts_on) as starts_on_day,
        case when discount_pct >= 0.33 then 'High' when discount_pct >= 0.15 then 'Medium' else 'Low' end as discount_pct_tier,
        coalesce(channel, 'UNKNOWN') as channel_filled
    from promotions
    where channel is not null
      and discount_pct between 0.15 and 0.17
)
-- Running and moving aggregates
select
    id,
    promo_code,
    discount_pct,
    starts_on_day,
    channel_filled,
    ntile(3) over (partition by channel order by starts_on_day, id) as starts_on_day_bucket,
    row_number() over (partition by promo_code order by starts_on desc, id) as seq_no
from base
where year(starts_on) = 2024;
