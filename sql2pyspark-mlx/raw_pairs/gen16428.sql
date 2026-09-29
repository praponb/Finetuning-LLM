with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v
    from carts
    lateral view explode(attributes) t as k, v
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by k order by created_at) as dedup_rank
    from flattened
    qualify dedup_rank = 1
),
enriched as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.created_at,
        a.k,
        a.v,
        a.dedup_rank,
        b.email
    from ranked a
    left join customers b
        on a.customer_id <=> b.id
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when created_at - lag(created_at) over (partition by k order by created_at, id, k) > interval 15 minutes then 1 else 0 end as is_new_session
    from enriched
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by k order by created_at, id, k rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when created_at - lag(created_at) over (partition by customer_id order by created_at, id, k) > interval 30 minutes then 1 else 0 end as is_new_session_2
    from islands
),
sessions_carts as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session_2) over (partition by customer_id order by created_at, id, k rows between unbounded preceding and current row) as burst_id_2
    from flagged
),
joined as (
    -- Attach customers attributes
    select
        s.id,
        s.customer_id,
        s.created_at,
        s.k,
        s.v,
        s.dedup_rank,
        s.is_new_session_2,
        p.country,
        p.email as customer_email
    from sessions_carts s
    left join customers p
        on s.customer_id = p.id
)
select
    s.id,
    s.customer_id,
    s.created_at,
    s.k,
    s.dedup_rank,
    s.is_new_session_2,
    s.customer_email,
    (select avg(o2.discount) from orders o2 where o2.customer_id = s.customer_id) as avg_order_discount
from joined s
order by is_new_session_2 desc nulls first;
