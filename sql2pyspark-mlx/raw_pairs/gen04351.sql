with valid as (
    select
        id,
        customer_id,
        store_id,
        channel,
        amount,
        discount,
        concat(status, '/', channel) as status_channel,
        coalesce(channel, 'OTHER') as resolved_channel
    from orders
    where store_id is not null
      and channel in ('store', 'web', 'partner')
),
enriched as (
    select
        a.id,
        a.customer_id,
        a.store_id,
        a.discount,
        a.status_channel,
        c.country,
        c.name,
        c.age
    from valid a
    inner join customers c
        on a.customer_id = c.id
    where c.is_active = false
),
with_windows as (
    select
        *,
        lag(age) over w_part as prev_age,
        lag(discount) over w_part as prev_discount,
        lead(discount) over w_part as next_discount,
        row_number() over w_part as event_seq
    from enriched
    window w_part as (partition by country, store_id order by age, id)
),
kept as (
    select s.*
    from with_windows s
    where exists (
            select 1
            from order_items x
            where x.order_id = s.id
              and x.quantity between 4 and 6
        )
),
joined as (
    select
        e.*,
        d.city,
        d.opened_date
    from kept e
    inner join stores d
        on e.store_id <=> d.id
),
filtered as (
    select *, next_discount + prev_age as next_discount_plus_prev_age
    from joined
    where city is not null
),
with_last as (
    select
        s.id,
        s.name,
        s.age,
        s.prev_age,
        s.prev_discount,
        s.next_discount,
        s.event_seq,
        s.city,
        s.opened_date,
        x.amount
    from filtered s
    left join lateral (
            select c.amount
            from payments c
            where c.order_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    name,
    age,
    prev_age,
    next_discount,
    city,
    opened_date,
    case when city rlike '^(New York|Tokyo)$' then 'OTHER_ZONE' when city rlike '^(Bangkok|Chiang Mai)$' then 'GROUP_B' else 'EXTENDED' end as city_zone
from with_last;
