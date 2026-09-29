with base as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        case when status = 'EXPIRED' then 1 else 0 end as is_expired,
        length(status) as status_length
    from subscriptions
    where start_date >= to_date('2023-05-01')
      and customer_id is not null
      and start_date between '2025-01-01' and '2025-03-20'
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from base
    qualify rank() over (partition by status order by start_date desc) = 1
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        lower(plan) as plan_lower,
        dayofweek(start_date) as start_date_dayofweek
    from ranked
    where end_date >= '2023-10-29'
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        lag(status_length, 1) over (partition by status order by start_date, id) as prev_status_length,
        last_value(start_date_dayofweek) ignore nulls over (partition by plan order by start_date_dayofweek, id rows between unbounded preceding and current row) as last_known_start_date_dayofweek,
        dense_rank() over (partition by status order by is_expired desc nulls first) as is_expired_dense_rank
    from cleaned
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from with_windows
    qualify row_number() over (partition by plan order by is_expired nulls last, id) = 1
),
deduped_subscriptions as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by status order by last_known_start_date_dayofweek desc nulls last) as rn
    from deduped
    qualify rn = 1
)
-- Native SQL PIVOT
select *
from (
    select
        plan,
        status,
        is_expired
    from deduped_subscriptions
) src
pivot (
    max(is_expired) as amount,
    count(1) as n
    for status in ('ACTIVE' as active, 'EXPIRED' as expired, 'CANCELLED' as cancelled)
);
