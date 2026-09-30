with kept as (
    select s.*
    from sessions s
    where not exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.event_type = 'add_to_cart'
        )
),
flagged as (
    select *, case when started_at - lag(started_at) over (partition by country order by started_at asc, id asc) > interval 7 days then 1 else 0 end as is_new_session
    from kept
),
sessions_sessions as (
    select
        id,
        customer_id,
        started_at,
        country,
        sum(is_new_session) over (partition by country order by started_at asc, id asc rows between unbounded preceding and current row) as burst_id
    from flagged
),
with_dims as (
    select
        e.id,
        e.customer_id,
        e.started_at,
        e.burst_id,
        d.city,
        d.signup_date,
        d.country as customer_country
    from sessions_sessions e
    inner join customers d
        on e.customer_id = d.id
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by city order by signup_date asc, id asc) as latest_rank
        from with_dims
    ) x
    where latest_rank = 1
)
select
    s.id,
    s.customer_id,
    s.started_at,
    s.burst_id,
    s.city,
    s.signup_date,
    s.customer_country,
    s.latest_rank,
    (select sum(o2.shipping_fee) from orders o2 where o2.customer_id = s.customer_id) as sum_order_shipping_fee
from deduped s
order by customer_country asc
