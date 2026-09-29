with base as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        plan,
        start_date,
        status,
        lower(status) as status_lower
    from subscriptions
    where start_date >= to_date('2023-07-14')
),
latest as (
    -- Keep one row per key (latest first)
    select
        id,
        customer_id,
        status_lower
    from (
        select *, row_number() over (partition by status order by start_date desc, id) as dedup_rank
        from base
    ) t
    where dedup_rank = 1
),
enriched as (
    -- Attach customers attributes
    select s.*, d.name
    from latest s
    left join customers d
        on s.customer_id = d.id
),
base_subscriptions as (
    -- Keep recent, valid rows
    select *, lower(name) as name_lower
    from enriched
    where customer_id is not null
)
select id, name_lower
from base_subscriptions;
