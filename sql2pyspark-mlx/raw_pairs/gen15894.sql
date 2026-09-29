with flattened as (
    select
        id,
        created_at,
        explode(coupon_codes) as code
    from carts
)
select
    date_trunc('day', created_at) as activity_day,
    code,
    count_if(code = 'VIP') as vip_events,
    count(*) as num_rows,
    size(collect_set(code)) as num_codes,
    count(*) filter (where code = 'BUNDLE') as bundle_rows
from flattened
group by date_trunc('day', created_at), code
order by activity_day
