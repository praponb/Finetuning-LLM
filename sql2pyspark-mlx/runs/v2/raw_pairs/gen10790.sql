with ranked as (
    select
        id,
        channel,
        discount,
        row_number() over (partition by channel order by discount, id) as rn
    from orders
)
select id, channel, discount
from ranked
where rn <= 3
order by channel;
