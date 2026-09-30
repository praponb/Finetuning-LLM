with valid as (
    -- Basic cleansing and defaults
    select
        id,
        starts_on,
        ends_on,
        length(channel) as channel_length,
        date_format(starts_on, 'yyyy-MM-dd') as starts_on_label,
        coalesce(channel, 'N/A') as channel_filled
    from promotions
    where channel is not null
      and promo_code <> 'SPRING10'
)
select id
from valid
intersect all
select id
from promotions
where channel in ('partner', 'mobile')
