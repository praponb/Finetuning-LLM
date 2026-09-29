with ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by status order by monthly_fee desc, id asc) as latest_rank
    from subscriptions
    qualify latest_rank = 1
)
select
    id,
    end_date,
    latest_rank
from ranked
order by end_date asc
