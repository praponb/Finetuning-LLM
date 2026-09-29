with latest as (
    -- QUALIFY-style window filter
    select *
    from subscriptions
    qualify rank() over (partition by status, customer_id order by end_date) = 1
)
select
    id,
    customer_id,
    plan,
    case when plan rlike '^(enterprise|standard)$' then 'PRIORITY' else 'GROUP_B' end as plan_zone
from latest
