with deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        customer_id,
        browser
    from sessions
    qualify rank() over (partition by device, customer_id order by started_at desc nulls first) = 1
)
select id
from deduped
