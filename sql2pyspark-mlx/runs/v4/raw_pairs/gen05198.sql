with kept_keys as (
    select id, channel
    from promotions
    intersect all
    select id, channel
    from promotions
    where discount_pct > 0.23
),
kept_rows as (
    select s.*
    from promotions s
    inner join kept_keys k on s.id = k.id and s.channel = k.channel
)
select
    id,
    channel,
    promo_code,
    ends_on,
    discount_pct
from kept_rows
order by id nulls last
