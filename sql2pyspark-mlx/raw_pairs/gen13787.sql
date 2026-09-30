with matched_keys as (
    -- Set intersection on keys
    select status, id
    from accounts
    intersect
    select status, id
    from accounts
    where customer_id in (17, 28)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from accounts s
    inner join matched_keys k on s.status = k.status and s.id = k.id
)
select
    s.id,
    s.customer_id,
    s.account_type,
    case when account_type rlike '^(CREDIT|SAVINGS)$' then 'CORE' else 'GROUP_B' end as account_type_zone,
    (select avg(s2.monthly_fee) from subscriptions s2 where s2.customer_id = s.customer_id) as avg_subscription_monthly_fee
from reconciled s
order by account_type_zone desc nulls last
