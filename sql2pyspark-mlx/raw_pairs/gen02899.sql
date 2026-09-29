with valid as (
    select
        id,
        customer_id,
        device,
        lower(device) as device_lower
    from sessions
    where customer_id is not null
      and browser is distinct from 'Chrome'
),
eligible as (
    select s.*
    from valid s
    where not exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.duration_sec > 412
        )
),
eligible_sessions as (
    select s.*
    from eligible s
    where exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.event_type != 'search'
        )
),
eligible_sessions_2 as (
    select s.*
    from eligible_sessions s
    where not exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.event_time < '2023-04-18'
        )
),
kept as (
    select s.*
    from eligible_sessions_2 s
    where exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.event_type in ('add_to_cart', 'search', 'purchase')
        )
)
select
    s.id,
    s.customer_id,
    s.device_lower,
    (select min(t2.discount) from orders t2 where t2.customer_id = s.customer_id) as min_order_discount
from kept s;
