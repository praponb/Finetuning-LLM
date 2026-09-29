with base as (
    select
        id,
        channel,
        starts_on,
        cast(discount_pct as int) as discount_pct_int,
        add_months(starts_on, 3) as starts_on_plus_months,
        coalesce(channel, 'N/A') as channel_filled
    from promotions
    where starts_on > date '2023-04-30'
      and channel is not null
)
select
    id,
    starts_on,
    discount_pct_int,
    channel_filled
from base
qualify row_number() over (partition by channel order by discount_pct_int desc, id) = 1;
